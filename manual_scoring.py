import numpy as np
import pandas as pd
import timeit
import re
from multiprocessing import Pool
from functools import partial

from bracket_io import _reconstruct_initial_bracket
from bracket_score import _unpack_bits, _decode_round_winners
from legacy import bracket_objects


# ---------------------------------------------------------------------------
# Vectorized scoring for a specific team (npz-based path)
# ---------------------------------------------------------------------------

def compute_all_scores_vectorized(results, brackets_npz, team_name, parquet_name):
    """For each bracket, record whether team_name reached the Elite Eight and Final Four.

    Parameters
    ----------
    results      : DataFrame from ESPN/538 (used only to identify the team)
    brackets_npz : path to the .npz file written by save_predictions_npz
    team_name    : team to track (e.g. "Saint Peter's")
    parquet_name : output path for the results parquet file
    """
    start = timeit.default_timer()

    data     = np.load(brackets_npz)
    bits_arr = data['bits']
    N        = len(bits_arr)
    print(f"Loaded {N} brackets")

    team_to_slot = {team: i for i, team in enumerate(_reconstruct_initial_bracket(data)['64'].values)}
    team_slot    = team_to_slot.get(team_name)
    if team_slot is None:
        raise ValueError(f"Team '{team_name}' not found in initial bracket")

    round_winners = _decode_round_winners(_unpack_bits(bits_arr))

    # round_winners[2] = Elite Eight (N, 8), round_winners[3] = Final Four (N, 4)
    pd.DataFrame({
        'name': [f'bracket{n}' for n in range(N)],
        '8':    (round_winners[2] == team_slot).any(axis=1).astype(np.int8),
        '4':    (round_winners[3] == team_slot).any(axis=1).astype(np.int8),
    }).to_parquet(parquet_name)

    print(f"Completed {N} brackets in {round(timeit.default_timer() - start, 2)}s")


# ---------------------------------------------------------------------------
# Legacy JSON-based scoring (kept for backward compatibility)
# ---------------------------------------------------------------------------

def _compute_score(obj, team_name, results, start_time):
    """Score a single bracket for a specific team from the JSON streaming format.

    Intended to be called via multiprocessing.Pool.map from compute_all_scores.

    Parameters
    ----------
    obj        : dict yielded by bracket_objects — keys: bracket name, value: [df_json, prob]
    team_name  : team to track
    results    : DataFrame (unused here; kept for signature compatibility)
    start_time : timeit timestamp used for progress logging

    Returns
    -------
    dict {bracket_name: [reached_elite_eight, reached_final_four]} (values 0 or 1)
    """
    bracket_name = list(obj)[0]
    bracket_num  = int(re.search(r'\d+', bracket_name).group(0))
    if bracket_num % 10000 == 0:
        print(f"{bracket_num}: {round(timeit.default_timer() - start_time, 2)}")

    values  = list(obj.values())
    bracket = pd.read_json(values[0][0])
    merged  = pd.merge(results, bracket, how='left', left_on='team_name', right_on='64')

    return {bracket_name: [
        int((merged["8"] == team_name).any()),
        int((merged["4"] == team_name).any()),
    ]}


def compute_all_scores(results, filename, team_name, parquet_name):
    """Parallel scoring of every bracket in a JSON file for a specific team.

    For large bracket files prefer compute_all_scores_vectorized which reads
    the npz format and is orders of magnitude faster.
    """
    start  = timeit.default_timer()
    pool   = Pool(processes=None)
    scores = pool.map(
        partial(_compute_score, team_name=team_name, results=results, start_time=start),
        bracket_objects(open(filename, 'rb')),
    )

    scores_dict = {k: v for dct in scores for k, v in dct.items()}
    print(f"Completed {len(scores)}: {round(timeit.default_timer() - start, 2)}")

    (pd.DataFrame.from_dict(scores_dict, orient='index', columns=["8", "4"])
       .reset_index()
       .rename(columns={'index': 'name'})
       .to_parquet(parquet_name))
