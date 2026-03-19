import pandas as pd
import timeit
from multiprocessing import Pool
from functools import partial
import re

from bracket_functions import (
    bracket_objects,
    score_bracket,
    num_specific_seed_in_specific_round,
)


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
    
    binary_bracket = pd.DataFrame({
        round_num: merged[round_num] != ""
        for round_num in ['32', '16', '8', '4', '2', '1']
    }).astype(int)
    
    seed_bracket = pd.DataFrame({
        seed: merged['seed'] == seed
        for seed in range(1, 17)
    }).astype(int)

    total_score, potential_score, num_correct = score_bracket(
        bracket=merged,
        binary_bracket=binary_bracket,
    )
    
    specific_seed_rounds = {
        f"num_{seed_num}_in_{round_num}": num_specific_seed_in_specific_round(
            binary_bracket=binary_bracket,
            seed_bracket=seed_bracket,
            seed_num=seed_num,
            round_num=round_num,
        )
        for seed_num in range(1, 17)
        for round_num in ["32", "16", "8", "4", "2", "1"]
    }
    df = pd.Series(specific_seed_rounds).reset_index().rename(columns={"index": "type", 0: "num_in_round"})
    df = pd.concat((
        df,
        df["type"].str.split("_", expand=True)[[1, 3]].rename(columns={1: "seed", 3: "round"}).astype(int)
    ), axis=1)
    best_seeds = {
        f"best_round_by_seed_{seed}": value
        for seed, value in df.loc[df["num_in_round"] > 0].groupby("seed")["round"].min().items()
    }
    
    return {
        "name": bracket_name,
        "likelihood": prob,
        "score": total_score,
        "potential_score": potential_score,
        **dict(zip(["32", "16", "8", "4", "2", "1"], num_correct)),
        **specific_seed_rounds,
        **best_seeds,
    }
    
#     scores = [prob, total_score, potential_score]
#     scores.extend([
#         num_specific_seed_in_specific_round(merged, seed_num=seed_num, round_num=round_num)
#         for seed_num in range(1, 17)
#         for round_num in ["32", "16", "8", "4"]
#     ])
#     scores.extend([
#         check_longest_by_seed(merged, seed_num=seed_num)
#         for seed_num in range(1, 17)
#     ])
#     scores.extend([item for item in num_correct])
#     return {bracket_name: scores}


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
#     scores_dict = {k: v for dct in scores for k, v in dct.items()}

    stop = timeit.default_timer()
    print("Completed {}: {}".format(len(scores), round(stop - start, 2)))

#     columns = [
#         "likelihood",
#         "score",
#         "16_seeds",
#         "16_total",
#         "potential_score",
#         '32',
#         '16',
#         '8',
#         '4',
#         '2',
#         '1',
#     ]

#     df_scores = (pd.DataFrame.from_dict(scores_dict, orient='index', columns=columns)
#                              .reset_index()
#                              .rename(columns={'index': 'name'}))

    pd.DataFrame(scores).to_feather(feather_name)
