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


def scrape_kenpom(year):
    """Scrape tournament teams from kenpom.com using a headless Firefox browser.

    Requires selenium, beautifulsoup4, and lxml plus geckodriver:
        pip install selenium beautifulsoup4 lxml
        brew install geckodriver

    Parameters
    ----------
    year : int  (currently must match the live KenPom season — no archive support)

    Returns
    -------
    DataFrame with columns: Seed (int), Team (str), AdjustEM (float), AdjustT (float)
    """
    try:
        from selenium import webdriver
        from bs4 import BeautifulSoup
    except ImportError as e:
        raise ImportError(
            "KenPom scraping requires selenium and beautifulsoup4. "
            "pip install selenium beautifulsoup4 lxml  &&  brew install geckodriver"
        ) from e

    COLUMNS = [
        'Rank', 'Team', 'Conference', 'W-L', 'AdjustEM',
        'AdjustO', 'AdjustO Rank', 'AdjustD', 'AdjustD Rank',
        'AdjustT', 'AdjustT Rank', 'Luck', 'Luck Rank',
        'SOS Pyth', 'SOS Pyth Rank', 'SOS OppO', 'SOS OppO Rank',
        'SOS OppD', 'SOS OppD Rank', 'NCSOS Pyth', 'NCSOS Pyth Rank',
    ]

    opts = webdriver.FirefoxOptions()
    opts.add_argument('--headless')
    browser = webdriver.Firefox(options=opts)
    try:
        browser.get('http://kenpom.com/index.php')
        soup = BeautifulSoup(browser.page_source, 'lxml')
    finally:
        browser.quit()

    df = pd.DataFrame(
        [
            [td.text for td in row.find_all('td')]
            for row in soup.find('table', {'id': 'ratings-table'}).find_all(class_='tourney')
        ],
        columns=COLUMNS,
    )

    def _parse_name(row):
        team_str = str(row['Team']).strip()
        for chars in [2, 1]:
            suffix = team_str[-chars:].strip()
            if suffix.isdigit() and 1 <= int(suffix) <= 16:
                row['Seed'] = int(suffix)
                row['Team'] = team_str[:-chars].strip()
                return row
        row['Seed'] = None
        return row

    df = df.apply(_parse_name, axis=1).dropna(subset=['Seed'])
    df['Seed']     = df['Seed'].astype(int)
    df['AdjustEM'] = pd.to_numeric(df['AdjustEM'], errors='coerce')
    df['AdjustT']  = pd.to_numeric(df['AdjustT'],  errors='coerce')
    return df[['Seed', 'Team', 'AdjustEM', 'AdjustT']].dropna().reset_index(drop=True)


def scrape_torvik(year, gender='womens'):
    """Scrape tournament teams from barttorvik.com using a headless Firefox browser.

    Requires selenium, beautifulsoup4, and lxml plus geckodriver.

    Parameters
    ----------
    year   : int — season year (e.g. 2026)
    gender : 'womens' uses ncaaw path; 'mens' uses ncaa path

    Returns
    -------
    DataFrame with columns: Team (str), Seed (int), pyth (float)
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup
    except ImportError as e:
        raise ImportError(
            "Torvik scraping requires selenium and beautifulsoup4. "
            "pip install selenium beautifulsoup4 lxml  &&  brew install geckodriver"
        ) from e

    path = 'ncaaw' if gender == 'womens' else 'ncaa'
    url  = f'https://barttorvik.com/{path}/trank.php?year={year}&conlimit=NCAA'

    opts = webdriver.FirefoxOptions()
    opts.add_argument('--headless')
    browser = webdriver.Firefox(options=opts)
    try:
        browser.get(url)
        WebDriverWait(browser, 10).until(
            EC.visibility_of_element_located((By.CLASS_NAME, 'seedrow'))
        )
        soup = BeautifulSoup(browser.page_source, 'lxml')
    finally:
        browser.quit()

    raw = pd.DataFrame(
        [[td.text for td in row.find_all('td')] for row in soup.find_all(class_='seedrow')]
    ).iloc[:, [1, 7]].rename(columns={7: 'pyth'})

    raw['Team'] = raw[1].str.split('\xa0\xa0\xa0').apply(lambda x: x[0])
    raw['Seed'] = (
        raw[1].str.split('\xa0\xa0\xa0')
        .apply(lambda x: x[1] if len(x) > 1 else None)
        .str.split(' seed').apply(lambda x: x[0] if x else None)
    )
    df = raw[['Team', 'Seed', 'pyth']].copy()
    df['Seed'] = pd.to_numeric(df['Seed'], errors='coerce')
    df['pyth'] = pd.to_numeric(df['pyth'], errors='coerce')
    return df.dropna().assign(Seed=lambda d: d['Seed'].astype(int)).reset_index(drop=True)


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