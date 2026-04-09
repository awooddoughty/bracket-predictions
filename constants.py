YEAR = 2026

# 2021 and 2022
# ORDER_OF_REGIONS = ['West', 'East', 'South', 'Midwest']  # This changes every year!
# # 2023
# ORDER_OF_REGIONS = {  # This changes every year!
#     "mens": ['South', 'East', 'Midwest', 'West'],
#     "womens": ["Greenville 1", "Seattle 4", "Greenville 2", "Seattle 3"]
# }
# 2024
# ORDER_OF_REGIONS = {  # This changes every year!
#     "mens": ['East', 'West', 'South', 'Midwest'],
#     "womens": ["Albany 1", "Portland 4", "Albany 2", "Portland 3"]
# }
# # 2025
# ORDER_OF_REGIONS = {  # This changes every year!
#     "mens": ['South', 'West', 'East', 'Midwest'],
#     "womens": ["Spokane 1", "Spokane 4", "Birmingham 2", "Birmingham 3"]
# }
# 2026
ORDER_OF_REGIONS = {  # This changes every year!
    "mens": ['East', 'South', 'West', 'Midwest'],
    "womens": ["Regional 1", "Regional 4", "Regional 2", "Regional 3"]
}


_BASE_DIR = "/Users/awooddoughty/Library/Mobile Documents/com~apple~CloudDocs/bracket_simple"


def get_paths(year, gender):
    """Return file paths for a given year and gender as a SimpleNamespace."""
    from types import SimpleNamespace
    d = f"{_BASE_DIR}/{year}/{gender}"
    return SimpleNamespace(
        dir=d,
        simple=f"{d}/simple_{year}.csv",
        kenpom=f"{d}/kenpom_{year}.csv",
        torvik=f"{d}/torvik_{year}.csv",
        npz=f"{d}/brackets_{year}.npz",
        parquet=f"{d}/brackets_{year}.parquet",
    )


ESPN_IDS = {
    2025: {
        "mens": 257,
        "womens": 258,
    },
    2026: {
        "mens": 277,
        "womens": 278,
    },
}


# DATA_DIRECTORY = (
#     f"/Volumes/3TB/bracket-data/{YEAR}/"
#     "{gender}"
# )
DATA_DIRECTORY = (
    f"/Users/awooddoughty/Library/Mobile Documents/com~apple~CloudDocs/bracket_simple/{YEAR}/"
    "{gender}"
)

INITIAL_FILE = f'{DATA_DIRECTORY}/initial_{YEAR}.xlsx'
SILVER_FILE = f'{DATA_DIRECTORY}/silver_{YEAR}.xlsx'

INITIAL_FILE_NEW = f'{DATA_DIRECTORY}/initial_{YEAR}.csv'
KENPOM_FILE = f'{DATA_DIRECTORY}/kenpom_{YEAR}.csv'
TORVIK_FILE = f'{DATA_DIRECTORY}/torvik_{YEAR}.csv'
ESPN_FILE = (
    'https://projects.fivethirtyeight.com/'
    f'march-madness-api/{YEAR}/fivethirtyeight_ncaa_forecasts.csv'
)
DATA_FILENAME = f'{DATA_DIRECTORY}/simple_{YEAR}.csv'

# Name mappings where fuzzy matching consistently fails.
# Keys are names as they appear in the ratings file (KenPom/Torvik).
# Values are names as they appear in the ESPN bracket.
# Add entries here when unmatched teams are printed during bracket creation.
NAME_OVERRIDES = {
    "mens": {
        'Connecticut':       'UConn',           # KenPom full name vs ESPN abbreviation
        'Ohio St.':          'Ohio State',       # KenPom abbreviates, ESPN does not
        'Iowa St.':          'Iowa State',
        'N.C. State':        'North Carolina State',
        'North Dakota St.':  'N Dakota St',
        'LIU':               'Long Island',
        'Utah St.': 'Utah State',
    },
    "womens": {
        'Connecticut':       'UConn',
        'Ohio St.':          'Ohio State',
        'Iowa St.':          'Iowa State',
        'Mississippi':       'Ole Miss',
        'Fairleigh Dickinson': 'FDU',
        'Western Illinois': 'W Illinois',
        'South Dakota St.': 'S Dakota St',
    },
}

BRACKETS_JSON = f'{DATA_DIRECTORY}/brackets_{YEAR}.json'
BRACKETS_NPZ = f'{DATA_DIRECTORY}/brackets_{YEAR}.npz'
BRACKETS_DIRECTORY = f'{DATA_DIRECTORY}/Brackets_{YEAR}'
BRACKETS_PARQUET = f'{DATA_DIRECTORY}/brackets_{YEAR}.parquet'
BRACKETS_FEATHER = f'{DATA_DIRECTORY}/brackets_{YEAR}.feather'

ESPN_GROUP_FILE = f'{DATA_DIRECTORY}/groups_{YEAR}.pkl'
ESPN_OUTPUT_DIRECTORY = f'{DATA_DIRECTORY}/espn_brackets_{YEAR}'
ESPN_OUTPUT_STUB = (
    f"{ESPN_OUTPUT_DIRECTORY}/"
    "espn_brackets_{{}}.json"
)
ESPN_SCORES = f'{DATA_DIRECTORY}/espn_scores.json'

ESPN_OUTPUT_NAME = f'{DATA_DIRECTORY}/espn_brackets_{YEAR}'