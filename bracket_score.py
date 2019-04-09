import pandas as pd
import timeit
from multiprocessing import Pool
from functools import partial
import re

from bracket_functions import (
    bracket_objects,
    score_bracket,
    num_16_seeds,
    check_longest_16,
)

ESPN_FILE = ('https://projects.fivethirtyeight.com/'
             'march-madness-api/2018/fivethirtyeight_ncaa_forecasts.csv')

# Update these!
# 538 does not update after the championship game
CHAMPIONSHIP_LOSER = 'Michigan'
CHAMPIONSHIP_WINNER = 'Villanova'

JSON_FILE = 'brackets_test.json'
FEATHER_FILE = 'brackets_test.feather'


def compute_score(obj, results, start_time):
    """Read saved bracket, merge results, compute scores, save to dict."""
    bracket_name = list(obj)[0]
    bracket_num = int(re.search('\d+', bracket_name).group(0))
    if bracket_num % 10000 == 0:
        stop_time = timeit.default_timer()
        print("{}: {}".format(bracket_num, round(stop_time - start_time, 2)))

    values = list(obj.values())
    bracket = pd.read_json(values[0][0])
    prob = float(values[0][1])

    merged = pd.merge(results, bracket, how='left', left_on='team_name', right_on='64')

    total_score, potential_score, num_correct = score_bracket(merged)

    total_16 = num_16_seeds(merged)

    longest_16 = check_longest_16(merged)

    scores = [prob, total_score, total_16, longest_16, potential_score]
    scores.extend([item for item in num_correct])
    return {bracket_name: scores}


def compute_all_scores(results, filename, feather_name):
    """Parallel scoring of every bracket in json. Write to dataframe."""
    num_cores = 22
    pool = Pool(processes=num_cores)

    start = timeit.default_timer()

    partial_bracket = partial(compute_score, results=results, start_time=start)
    scores = pool.map(partial_bracket, bracket_objects(open(filename, 'rb')))
    # scores = []
    # for bracket in bracket_objects(open(filename, 'rb')):
    #     scores.append(partial_bracket(bracket))

    # convert list of dicts to one big dict
    scores_dict = {k: v for dct in scores for k, v in dct.items()}

    stop = timeit.default_timer()
    print("Completed {}: {}".format(len(scores), round(stop - start, 2)))

    columns = [
        "likelihood",
        "score",
        "16_seeds",
        "16_total",
        "potential_score",
        '32',
        '16',
        '8',
        '4',
        '2',
        '1',
    ]

    df_scores = (pd.DataFrame.from_dict(scores_dict, orient='index', columns=columns)
                             .reset_index()
                             .rename(columns={'index': 'name'}))

    df_scores.to_feather(feather_name)


if __name__ == '__main__':

    espn = pd.read_csv(ESPN_FILE)
    most_recent = max(espn[espn['gender'] == 'mens']['forecast_date'])
    results = espn[
        (espn['gender'] == 'mens') &
        (espn['forecast_date'] == most_recent) &
        (espn['rd1_win'] == 1)
    ]

    results.loc[results['team_name'] == CHAMPIONSHIP_LOSER, 'rd7_win'] = 0
    results.loc[results['team_name'] == CHAMPIONSHIP_WINNER, 'rd7_win'] = 1

    compute_all_scores(results, JSON_FILE, FEATHER_FILE)
