import pandas as pd
import timeit
from multiprocessing import Pool
from functools import partial
import re

from bracket_functions import (
    bracket_objects,
)


def check_bracket_result(merged, team, round_):
    return int((merged[round_] == team).any())


def compute_score(obj, team_name, results, start_time):
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

    v1 = check_bracket_result(merged=merged, team=team_name, round_="8")
    v2 = check_bracket_result(merged=merged, team=team_name, round_="4")
    return {bracket_name: [v1, v2]}


def compute_all_scores(results, filename, team_name, feather_name):
    """Parallel scoring of every bracket in json. Write to dataframe."""
    num_cores = 22
    pool = Pool(processes=num_cores)

    start = timeit.default_timer()

    partial_bracket = partial(compute_score, team_name=team_name, results=results, start_time=start)
    scores = pool.map(partial_bracket, bracket_objects(open(filename, 'rb')))
#     scores = []
#     for bracket in bracket_objects(open(filename, 'rb')):
#         scores.append(partial_bracket(bracket))

    # convert list of dicts to one big dict
    scores_dict = {k: v for dct in scores for k, v in dct.items()}

    stop = timeit.default_timer()
    print("Completed {}: {}".format(len(scores), round(stop - start, 2)))

    columns = [
        "8",
        "4",
    ]

    df_scores = (pd.DataFrame.from_dict(scores_dict, orient='index', columns=columns)
                             .reset_index()
                             .rename(columns={'index': 'name'}))

    df_scores.to_feather(feather_name)
