import numpy as np
import pandas as pd
from fuzzywuzzy import process

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns', 0)  # Display any number of columns
pd.set_option('display.max_rows', 68)  # Display any number of rows

KENPOM_FILE = 'kenpom.csv'
ESPN_FILE = ('https://projects.fivethirtyeight.com/'
             'march-madness-api/2019/fivethirtyeight_ncaa_forecasts.csv')
EXPORT_FILENAME = 'simple_2018.csv'
FIRST_FOUR_LOSERS = [
    "St. John's (NY)",
    'Temple',
    'North Carolina Central',
    'Prairie View',
]

# Load kenpom data that was previously scraped
kenpom = pd.read_csv(KENPOM_FILE)

# Import 538's info
espn = pd.read_csv(ESPN_FILE)
espn_slice = (espn['gender'] == 'mens') & (espn['forecast_date'] == max(espn['forecast_date']))
team_info = espn[espn_slice][['team_id', 'team_name', 'team_region', 'team_seed']]

# Match Kenpom and ESPN team names
kenpom['team_id'] = np.nan
kenpom['espn_name'] = np.nan
choices = team_info['team_name'].tolist()  # Get all of the ESPN team names
for index, row in kenpom.iterrows():
    team = row['Team']
    match = process.extractOne(team, choices)  # extract the best match for every kenpom name
    if match[1] == 100:  # it's a perfect match
        espn_team_id = team_info[team_info['team_name'] == match[0]]['team_id'].values[0]
        kenpom.loc[index, 'team_id'] = espn_team_id
        choices.remove(match[0])  # remove that team as a possible choice

# Need to do this first, otherwise the >80 match fails since it matches to Iowa St. ¯\_(ツ)_/¯
kenpom.loc[kenpom['Team'] == "St. John's", 'team_id'] = 2599
choices.remove("St. John's (NY)")


scores = {}
for index, row in kenpom[kenpom['team_id'].isna()].iterrows():
    team = row['Team']
    match = process.extractOne(team, choices)
    espn_team_id = team_info[team_info['team_name'] == match[0]]['team_id'].values[0]
    scores[team] = match, espn_team_id
    if match[1] > 80:
        kenpom.loc[index, 'team_id'] = espn_team_id
        choices.remove(match[0])  # remove that team as a possible choice

missing_kenpom = kenpom.loc[kenpom['team_id'].isna(), 'Team'].tolist()
missing_espn = team_info.loc[team_info['team_name'].isin(choices)][['team_name', 'team_id']]

print("Missing Kenpom Teams: {}".format(missing_kenpom))
print("Matching ESPN Teams:\n {}".format(missing_espn))

# Need to manually fix the crappy ones
kenpom.loc[kenpom['Team'] == 'LSU', 'team_id'] = 99
kenpom.loc[kenpom['Team'] == 'VCU', 'team_id'] = 2670
kenpom.loc[kenpom['Team'] == 'UCF', 'team_id'] = 2116

merged = pd.merge(kenpom, team_info, how='left', on='team_id')

for team in FIRST_FOUR_LOSERS:
    merged = merged[merged['team_name'] != team]

simple = merged[['team_region', 'Seed', 'team_name', 'team_id', 'AdjustEM', 'AdjustT']]

simple.to_csv(EXPORT_FILENAME, index=False)
