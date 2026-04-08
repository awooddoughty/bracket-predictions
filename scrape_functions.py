import pandas as pd
import requests

ESPN_FILE = "https://gambit-api.fantasy.espn.com/apis/v1/propositions/?challengeId={ID}&platform=chui&view=chui_default"


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