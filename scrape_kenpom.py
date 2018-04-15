__author__ = 'Alex'

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re

url = 'http://kenpom.com/index.php'

f = requests.get(url)
soup = BeautifulSoup(f.text, "lxml")

table_html = soup.find_all('table', {'id': 'ratings-table'})

# Weird issue w/ <thead> in the html
# Prevents us from just using pd.read_html
# Let's find all the thead contents and just replace/remove them
# This allows us to easily put the table row data into a dataframe using panda
thead = table_html[0].find_all('thead')

table = table_html[0]
for x in thead:
    table = str(table).replace(str(x), '')

#    table = "<table id='ratings-table'>%s</table>" % table
df = pd.read_html(table, converters={3:lambda x: str(x)})[0]

df.columns = ['Rank', 'Team', 'Conference', 'W-L', 'AdjustEM',
             'AdjustO', 'AdjustO Rank', 'AdjustD', 'AdjustD Rank',
             'AdjustT', 'AdjustT Rank', 'Luck', 'Luck Rank',
             'SOS Pyth', 'SOS Pyth Rank', 'SOS OppO', 'SOS OppO Rank',
             'SOS OppD', 'SOS OppD Rank', 'NCSOS Pyth', 'NCSOS Pyth Rank']

# Lambda that returns true if given string is a number and a valid seed number (1-16)
valid_seed = lambda x: True if str(x).replace(' ', '').isdigit() \
                and int(x) > 0 and int(x) <= 16 else False

# Use lambda to parse out seed/team
df['Seed'] = df['Team'].apply(lambda x: x[-2:].replace(' ', '') \
                              if valid_seed(x[-2:]) else np.nan )

df['Team'] = df['Team'].apply(lambda x: x[:-2] if valid_seed(x[-2:]) else x)

# Split W-L column into wins and losses
df['Wins'] = df['W-L'].apply(lambda x: int(re.sub('-.*', '', x)) )
df['Losses'] = df['W-L'].apply(lambda x: int(re.sub('.*-', '', x)) )
df.drop('W-L', inplace=True, axis=1)

# Reorder columns just cause I'm OCD
df=df[[ 'Rank', 'Seed', 'Team', 'Conference', 'Wins', 'Losses', 'AdjustEM',
             'AdjustO', 'AdjustO Rank', 'AdjustD', 'AdjustD Rank',
             'AdjustT', 'AdjustT Rank', 'Luck', 'Luck Rank',
             'SOS Pyth', 'SOS Pyth Rank', 'SOS OppO', 'SOS OppO Rank',
             'SOS OppD', 'SOS OppD Rank', 'NCSOS Pyth', 'NCSOS Pyth Rank']]

# Drop non tournament teams
df = df.dropna()

df.to_csv('kenpom.csv', index=False)
