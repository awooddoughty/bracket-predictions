import numpy as np
import pandas as pd
import timeit
import re
from multiprocessing import Pool
from functools import partial

from bracket_io import _reconstruct_initial_bracket
from legacy import bracket_objects, score_bracket, num_specific_seed_in_specific_round


# ---------------------------------------------------------------------------
# Vectorized scoring (npz-based path)
# ---------------------------------------------------------------------------

def _unpack_bits(bits_arr):
    """Unpack (N,) uint64 array to (N, 63) bool array, one column per game."""
    bit_indices = np.arange(63, dtype=np.uint64)
    return ((bits_arr[:, None] >> bit_indices[None, :]) & np.uint64(1)).astype(bool)


def _decode_round_winners(bit_matrix):
    """Decode bit matrix to per-round winner slot indices.

    Parameters
    ----------
    bit_matrix : (N, 63) bool

    Returns
    -------
    List of six arrays, one per round:
      round_winners[0] : (N, 32) uint8  — original slot of each Round-of-32 winner
      round_winners[1] : (N, 16) uint8  — Sweet 16 winners
      round_winners[2] : (N,  8) uint8  — Elite Eight winners
      round_winners[3] : (N,  4) uint8  — Final Four winners
      round_winners[4] : (N,  2) uint8  — Championship game teams
      round_winners[5] : (N,  1) uint8  — Champion
    """
    round_winners = []
    offset = 0

    # Round 0 (R64 → R32): game i is slot 2i vs slot 2i+1
    base = np.arange(0, 64, 2, dtype=np.uint8)
    w    = base[None, :] + bit_matrix[:, :32].astype(np.uint8)  # (N, 32)
    round_winners.append(w)
    offset = 32

    # Rounds 1–5: game i pairs prev_winners[:, 2i] vs prev_winners[:, 2i+1]
    prev = w
    for n_games in [16, 8, 4, 2, 1]:
        bits = bit_matrix[:, offset:offset + n_games]
        w    = np.where(bits, prev[:, 1::2], prev[:, 0::2]).astype(np.uint8)
        round_winners.append(w)
        prev   = w
        offset += n_games

    return round_winners


def _build_actual_matrices(results, team_to_slot):
    """Build (64, 6) bool matrices for actual wins and potential wins.

    actual_won[slot, r]        = True if the team actually won round r
    potentially_alive[slot, r] = True if the team could still win round r (result > 0)
    """
    actual_won        = np.zeros((64, 6), dtype=bool)
    potentially_alive = np.zeros((64, 6), dtype=bool)
    rd_cols = ['rd2_win', 'rd3_win', 'rd4_win', 'rd5_win', 'rd6_win', 'rd7_win']

    for _, row in results.iterrows():
        slot = team_to_slot.get(row['team_name'])
        if slot is None:
            continue
        for r_idx, col in enumerate(rd_cols):
            val = float(row.get(col, 0) or 0)
            if val == 1.0:
                actual_won[slot, r_idx]        = True
                potentially_alive[slot, r_idx] = True
            elif val > 0.0:
                potentially_alive[slot, r_idx] = True

    return actual_won, potentially_alive


def compute_all_scores_vectorized(results, brackets_npz, parquet_name):
    """Score all brackets in an npz file using vectorized numpy operations.

    This replaces the slow ijson-based compute_all_scores.  For 1M brackets it
    runs in seconds rather than hours.

    Parameters
    ----------
    results      : DataFrame from ESPN/538 with team_name and rd*_win columns
    brackets_npz : path to the .npz file written by save_predictions_npz
    parquet_name : output path for the scored brackets parquet file
    """
    start = timeit.default_timer()

    data     = np.load(brackets_npz)
    bits_arr = data['bits']   # (N,) uint64
    probs    = data['probs']  # (N,) float64
    N        = len(bits_arr)
    print(f"Loaded {N} brackets")

    initial_bracket = _reconstruct_initial_bracket(data)
    team_to_slot    = {team: i for i, team in enumerate(initial_bracket['64'].values)}
    slot_seeds      = initial_bracket['Seed'].values.astype(int)

    actual_won, potentially_alive = _build_actual_matrices(results, team_to_slot)

    bit_matrix    = _unpack_bits(bits_arr)
    round_winners = _decode_round_winners(bit_matrix)

    pts_per_round = np.array([10, 20, 40, 80, 160, 320], dtype=np.int32)
    round_labels  = ['32', '16', '8', '4', '2', '1']

    scores            = np.zeros(N, dtype=np.int32)
    potential_scores  = np.zeros(N, dtype=np.int32)
    round_correct     = np.zeros((N, 6), dtype=np.int8)
    seed_round_counts = np.zeros((N, 16, 6), dtype=np.int8)

    for r_idx, winners in enumerate(round_winners):
        n_games     = winners.shape[1]
        predicted_r = np.zeros((N, 64), dtype=bool)
        predicted_r[np.repeat(np.arange(N), n_games), winners.ravel().astype(int)] = True

        correct_r   = predicted_r & actual_won[None, :, r_idx]
        potential_r = predicted_r & potentially_alive[None, :, r_idx]

        round_correct[:, r_idx]  = correct_r.sum(axis=1).astype(np.int8)
        scores           += correct_r.sum(axis=1).astype(np.int32)   * pts_per_round[r_idx]
        potential_scores += potential_r.sum(axis=1).astype(np.int32) * pts_per_round[r_idx]

        for s in range(16):
            seed_round_counts[:, s, r_idx] = (
                predicted_r & (slot_seeds == s + 1)[None, :]
            ).sum(axis=1).astype(np.int8)

    round_label_vals = np.array([32, 16, 8, 4, 2, 1], dtype=np.int32)

    df_dict = {
        'name':            [f'bracket{n}' for n in range(N)],
        'likelihood':      probs,
        'score':           scores,
        'potential_score': potential_scores,
    }
    for r_idx, label in enumerate(round_labels):
        df_dict[label] = round_correct[:, r_idx]

    for s in range(16):
        for r_idx, label in enumerate(round_labels):
            df_dict[f'num_{s + 1}_in_{label}'] = seed_round_counts[:, s, r_idx]

    for s in range(16):
        has_seed  = seed_round_counts[:, s, :] > 0
        best_vals = round_label_vals[5 - has_seed[:, ::-1].argmax(axis=1)].astype(float)
        best_vals[~has_seed.any(axis=1)] = np.nan
        df_dict[f'best_round_by_seed_{s + 1}'] = best_vals

    pd.DataFrame(df_dict).to_parquet(parquet_name)
    print(f"Completed {N} brackets in {round(timeit.default_timer() - start, 2)}s")


# ---------------------------------------------------------------------------
# Legacy JSON-based scoring (kept for backward compatibility)
# ---------------------------------------------------------------------------

def compute_score(obj, results, start_time):
    """Score a single bracket from the JSON streaming format.

    Intended to be called via multiprocessing.Pool.map from compute_all_scores.

    Parameters
    ----------
    obj        : dict yielded by bracket_objects — keys: bracket name, value: [df_json, prob]
    results    : DataFrame from ESPN/538 with team_name and rd*_win columns
    start_time : timeit timestamp used for progress logging

    Returns
    -------
    dict with keys: name, likelihood, score, potential_score, '32'..'1',
    num_{seed}_in_{round}, best_round_by_seed_{seed}
    """
    bracket_name = list(obj)[0]
    bracket_num  = int(re.search(r'\d+', bracket_name).group(0))
    if bracket_num % 10000 == 0:
        print(f"{bracket_num}: {round(timeit.default_timer() - start_time, 2)}")

    values  = list(obj.values())
    bracket = pd.read_json(values[0][0])
    prob    = float(values[0][1])

    merged = pd.merge(results, bracket, how='left', left_on='team_name', right_on='64')

    binary_bracket = pd.DataFrame({
        r: merged[r] != "" for r in ['32', '16', '8', '4', '2', '1']
    }).astype(int)

    seed_bracket = pd.DataFrame({
        seed: merged['seed'] == seed for seed in range(1, 17)
    }).astype(int)

    total_score, potential_score, num_correct = score_bracket(merged, binary_bracket)

    specific_seed_rounds = {
        f"num_{seed_num}_in_{round_num}": num_specific_seed_in_specific_round(
            binary_bracket, seed_bracket, seed_num, round_num,
        )
        for seed_num in range(1, 17)
        for round_num in ["32", "16", "8", "4", "2", "1"]
    }
    df = pd.Series(specific_seed_rounds).reset_index().rename(columns={"index": "type", 0: "num_in_round"})
    df = pd.concat((
        df,
        df["type"].str.split("_", expand=True)[[1, 3]].rename(columns={1: "seed", 3: "round"}).astype(int),
    ), axis=1)
    best_seeds = {
        f"best_round_by_seed_{seed}": value
        for seed, value in df.loc[df["num_in_round"] > 0].groupby("seed")["round"].min().items()
    }

    return {
        "name":            bracket_name,
        "likelihood":      prob,
        "score":           total_score,
        "potential_score": potential_score,
        **dict(zip(["32", "16", "8", "4", "2", "1"], num_correct)),
        **specific_seed_rounds,
        **best_seeds,
    }


def compute_all_scores(results, filename, parquet_name):
    """Parallel scoring of every bracket in a JSON file.

    For large bracket files prefer compute_all_scores_vectorized which reads
    the npz format and is orders of magnitude faster.
    """
    start = timeit.default_timer()
    pool  = Pool(processes=None)

    scores = pool.map(
        partial(compute_score, results=results, start_time=start),
        bracket_objects(open(filename, 'rb')),
    )

    print(f"Completed {len(scores)}: {round(timeit.default_timer() - start, 2)}")
    pd.DataFrame(scores).to_parquet(parquet_name)
