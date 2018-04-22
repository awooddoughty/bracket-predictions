__author__ = 'Alex'

import numpy as np
import pandas as pd
import timeit
from multiprocessing import Pool
from functools import partial
import re

from bracket_functions import (
    bracket_objects,
    score_bracket,
    check_umbc,
    num_16_seeds,
    check_longest_16,
)

espn = pd.read_csv('https://projects.fivethirtyeight.com/march-madness-api/2018/fivethirtyeight_ncaa_forecasts.csv')
most_recent = max(espn[espn['gender']=='mens']['forecast_date'])
results = espn[(espn['gender']=='mens') & (espn['forecast_date'] == most_recent) & (espn['rd1_win'] == 1)]

# Update these!
# 538 does not update after the championship game
results.loc[results['team_name'] == 'Michigan', 'rd7_win'] = 0
results.loc[results['team_name'] == 'Villanova', 'rd7_win'] = 1


def compute_score(obj, results, start_time):
    bracket_name = obj.keys()[0]
    bracket_num = int(re.search('\d+', bracket_name).group(0))
    if bracket_num % 100 == 0:
        stop_time = timeit.default_timer()
        print bracket_num, ' ', stop_time - start_time

    bracket = pd.read_json(obj.values()[0][0])
    prob = float(obj.values()[0][1])

    merged = pd.merge(results, bracket, how='left', left_on='team_name', right_on='64')

    total_score, potential_score, num_correct = score_bracket(merged)

    umbc = check_umbc(bracket)

    total_16 = num_16_seeds(merged)

    longest_16 = check_longest_16(merged)

    scores = [prob, total_score, total_16, longest_16, umbc, potential_score]
    scores.extend([item for item in num_correct])
    return {bracket_name: scores}


def compute_all_scores(filename, feather_name):
    num_cores = 22
    pool = Pool(processes=num_cores)

    start = timeit.default_timer()

    partial_bracket = partial(compute_score, results=results, start_time=start)
    scores = pool.map(partial_bracket, bracket_objects(open(filename)))

    scores_dict = reduce(lambda r, d: r.update(d) or r, scores, {})

    stop = timeit.default_timer()
    print len(scores), ' ', stop - start

    columns = [
        "likelihood",
        "score",
        "16_seeds",
        "16_total",
        "umbc",
        "potential_score",
        '32',
        '16',
        '8',
        '4',
        '2',
        '1',
    ]

    col_dict = {i: col for i, col in enumerate(columns)}
    col_dict.update({'index': 'name'})

    df_scores = pd.DataFrame.from_dict(scores_dict, orient='index').reset_index().rename(index=str, columns=col_dict)

    df_scores.reset_index().to_feather(feather_name)


if __name__ == '__main__':
    filename = 'brackets_test.json'
    feather_name = 'brackets_test.feather'

    compute_all_scores(filename, feather_name)



