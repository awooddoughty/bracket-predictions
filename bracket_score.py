__author__ = 'Alex'

import numpy as np
import pandas as pd
import timeit
import json
from multiprocessing import Pool
from functools import partial
import re
import ijson.backends.yajl2_cffi as ijson
# import ijson
from itertools import islice
import seaborn as sns
import matplotlib.pyplot as plt
import os

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns', 0) # Display any number of columns
pd.set_option('display.max_rows', 68) # Display any number of rows

# filename = 'brackets.json'
# with open(filename, 'r') as f:
#     objects = ijson.items(f, 'meta.view.columns.item')
#     columns = list(objects)
#
# brackets = json.load(open('brackets.json'))

espn = pd.read_csv('https://projects.fivethirtyeight.com/march-madness-api/2018/fivethirtyeight_ncaa_forecasts.csv')
most_recent = max(espn[espn['gender']=='mens']['forecast_date'])
results = espn[(espn['gender']=='mens') & (espn['forecast_date'] == most_recent) & (espn['rd1_win'] == 1)]

# Update these!
# 538 does not update after the championship game
results.loc[results['team_name'] == 'Michigan', 'rd7_win'] = 0
results.loc[results['team_name'] == 'Villanova', 'rd7_win'] = 1


def compute_score(line, results, start_time):
    try:
        if line[1] == 'string':
            bracket_name = line[0]
            bracket_num = int(re.search('\d+', bracket_name).group(0))
            if bracket_num % 100 == 0:
                stop_time = timeit.default_timer()
                print bracket_num, ' ', stop_time-start_time

            bracket = pd.read_json(line[2])

            merged = pd.merge(results, bracket,how='left',left_on='team_name', right_on='64')

            total_score = 0
            potential_score = 0
            num_correct = []
            for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
                round_num = str(index+2)
                binary_round = merged[round] != ''
                results_round = merged['rd'+round_num+'_win'] == 1
                potential_round = merged['rd'+round_num+'_win'] > 0
                score_vec = binary_round & results_round
                potential_vec = binary_round & potential_round
                pts_per_correct = 10*(2**index)      #ESPN scoring
                score = score_vec.sum() * pts_per_correct
                pot_score = potential_vec.sum() * pts_per_correct
                total_score += score
                potential_score += pot_score
                num_correct.append(score_vec.sum())

            # Check UMBC
            umbc = 0
            if bracket.loc[1, '32'] != "":
                umbc = 1

            # check 16 seeds
            # total_16=0
            merged['seed'] = merged['team_seed'].apply(lambda x: int(filter(str.isdigit, x)))
            binary_round = merged['32']!=''
            seed_round = merged['seed']==16
            score_vec = binary_round & seed_round
            total_16 = score_vec.sum()

            longest_16 = 0
            for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
                binary_round = merged[round]!=''
                seed_round = merged['seed']==16
                score_vec = binary_round & seed_round
                pts_per_correct = 10*(2**index)      #ESPN scoring
                score = score_vec.sum() * pts_per_correct
                longest_16 += score

            scores = [total_score, total_16, longest_16, umbc, potential_score]
            scores.extend([item for item in num_correct])
            return {bracket_name: scores}

        else:
            return {}
    except Exception:
        return {}


sub_brackets = 1000002
# sub_brackets = 1002
filename = 'brackets_2018.json'

num_cores = 22
pool = Pool(processes=num_cores)

start = timeit.default_timer()

partial_bracket = partial(compute_score, results=results, start_time=start)
scores = pool.map(partial_bracket, islice(ijson.parse(open(filename)), sub_brackets*2))
# scores = []
# bracket_json = ijson.parse(open(filename))
# for line in islice(bracket_json, sub_brackets*2):
#     scores.append(compute_score(line, results, start))
scores_dict = reduce(lambda r, d: r.update(d) or r, scores, {})

stop = timeit.default_timer()
print sub_brackets, ' ', stop - start

df_scores = pd.DataFrame.from_dict(scores_dict, orient='index').reset_index().rename(index=str, columns={"index": "Name", 0: "Score", 1:"16 Seeds", 2:"16 Total", 3:"UMBC", 4:"Potential Score", 5: '32', 6: '16', 7: '8', 8: '4', 9: '2', 10: '1'})
print df_scores[df_scores['Score']==df_scores['Score'].max()]
print df_scores[df_scores['16 Seeds']==df_scores['16 Seeds'].max()]
print df_scores[df_scores['16 Total']==df_scores['16 Total'].max()]
print df_scores[df_scores['UMBC']==df_scores['UMBC'].max()]
# print df_scores[df_scores['Potential Score']==df_scores['Potential Score'].max()]
print df_scores[df_scores['32']==df_scores['32'].max()]
# print df_scores[df_scores['16']==df_scores['16'].max()]
# print df_scores[df_scores['8']==df_scores['8'].max()]
# print df_scores[df_scores['4']==df_scores['4'].max()]
# print sum(df_scores['2']==df_scores['2'].max())
# print sum(df_scores['1']==df_scores['1'].max())
print df_scores.shape


def save_brackets(folder, bracket_names):
    directory = os.path.dirname('Brackets_2018/' + folder + '/' + 'test')
    if not os.path.exists(directory):
        os.makedirs(directory)
    bracket_json = ijson.parse(open(filename))
    for line in islice(bracket_json, sub_brackets*2):
        if line[0] in bracket_names:
            bracket_name = line[0]
            bracket = pd.read_json(line[2])
            bracket = bracket[['32', '16', '8', '4', '2', '1']]
            bracket.to_html('Brackets_2018/' + folder + '/' + bracket_name + '.html', border=0)


max_scores = df_scores[df_scores['Score']==df_scores['Score'].max()]
bracket_names_top = max_scores['Name'].tolist()
save_brackets('top', bracket_names_top)

sixteen_seeds = df_scores[df_scores['16 Total']==df_scores['16 Total'].max()]
bracket_names_16 = sixteen_seeds['Name'].tolist()
save_brackets('16_seed', bracket_names_16)

first_round = df_scores[df_scores['32']==df_scores['32'].max()]
bracket_names_first = first_round['Name'].tolist()
save_brackets('first_round', bracket_names_first)

df_scores.reset_index().to_feather('scores_2018.feather')

pd.options.display.max_rows = 999
print df_scores.Score.value_counts()
sns.distplot(df_scores.Score)
plt.xticks(range(0, 1670, 250), fontsize=8)
plt.show()