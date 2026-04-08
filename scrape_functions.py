import pandas as pd
import requests

ESPN_FILE = "https://gambit-api.fantasy.espn.com/apis/v1/propositions/?challengeId={ID}&platform=chui&view=chui_default"


def match_team_names(ratings_df, espn_names, overrides=None, name_col='Team', threshold=85):
    """Match team names from a ratings source (KenPom/Torvik) to ESPN team names.

    Parameters
    ----------
    ratings_df  : DataFrame containing a column of team names
    espn_names  : list of ESPN team name strings to match against
    overrides   : dict {ratings_name: espn_name} for known problem pairs that
                  fuzzy matching consistently gets wrong.  Add entries to
                  NAME_OVERRIDES in constants.py and re-run when unmatched
                  teams are printed below.
    name_col    : column in ratings_df containing team names (default 'Team')
    threshold   : minimum fuzzy score to auto-accept a match (default 85)

    Returns
    -------
    mapping           : dict {ratings_name: espn_name} for all matched teams
    unmatched_ratings : list of ratings names with no ESPN match found
    unmatched_espn    : list of ESPN names that were not claimed by any rating
    """
    from fuzzywuzzy import process

    remaining_espn = list(espn_names)
    mapping = {}

    # Apply known overrides first so they can't be stolen by fuzzy matching
    for r_name, espn_name in (overrides or {}).items():
        if espn_name in remaining_espn:
            mapping[r_name] = espn_name
            remaining_espn.remove(espn_name)

    # Score all remaining pairs, then greedily assign best matches first
    # (sorting by score prevents a weak early match from claiming a name
    # that a stronger later match needs)
    unmatched = [name for name in ratings_df[name_col] if name not in mapping]
    candidates = [
        (score, r_name, espn_name)
        for r_name in unmatched
        for espn_name, score in [process.extractOne(r_name, remaining_espn)]
        if remaining_espn
    ]
    for score, r_name, espn_name in sorted(candidates, reverse=True):
        if score >= threshold and espn_name in remaining_espn:
            mapping[r_name] = espn_name
            remaining_espn.remove(espn_name)

    unmatched_ratings = [name for name in ratings_df[name_col] if name not in mapping]
    return mapping, unmatched_ratings, remaining_espn


def scrape_espn_data(id_):
    return requests.get(ESPN_FILE.format(ID=id_)).json()


def parse_bracket_data(res):
    team_won_rounds = {}
    team_open_rounds = {}
    all_teams = []

    for game in res:
        period = game.get('scoringPeriodId') + 1
        status = game.get('status')
        correct_ids = set(game.get('correctOutcomes') or [])

        for outcome in game.get('possibleOutcomes', []):
            oid = outcome.get('id')
            name = outcome.get('name', '')
            region_id = outcome.get('regionId')
            seed = outcome.get('regionSeed')

            if status != 'COMPLETE':
                team_open_rounds.setdefault(name, set()).add(period)
            elif oid in correct_ids:
                team_won_rounds.setdefault(name, set()).add(period)
            all_teams.append((name, region_id, seed))

    all_teams = set(all_teams)
    results = []
    for team, region_id, seed in all_teams:
        won = team_won_rounds.get(team, set())
        open_r = team_open_rounds.get(team, set())
        max_won = max(won) if won else 1
        expected_next = max_won + 1
        is_active = expected_next in open_r
        furthest = max_won if max_won > 0 else (min(open_r) if open_r else 0)
        if is_active:
            furthest = expected_next
        results.append({
            'team_name': team,
            'region_id': region_id,
            'seed': seed,
            'furthest_round': furthest,
            'is_active': is_active,
        })
    df = pd.DataFrame(results)
    for rnd in range(1, 8):
        col = f'rd{rnd}_win'
        def win_val(row, rnd=rnd):
            if row['furthest_round'] > rnd:
                return 1
            elif row['furthest_round'] == rnd:
                return 0.5 if row['is_active'] else 1
            elif row['is_active']:
                return 0.5
            else:
                return 0
        df[col] = df.apply(win_val, axis=1)
    return df