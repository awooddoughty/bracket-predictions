import os
import re
import numpy as np
import pandas as pd

from bracket_sim import decode_bracket


def _reconstruct_initial_bracket(npz_data):
    """Rebuild the initial bracket DataFrame from arrays stored in an npz file."""
    return pd.DataFrame({
        'Region': npz_data['slot_regions'].astype(str),
        'Seed':   npz_data['slot_seeds'].astype(int),
        '64':     npz_data['slot_teams'].astype(str),
        '32': '', '16': '', '8': '', '4': '', '2': '', '1': '',
    })


def save_predictions_npz(path, bits_arr, probs_arr, initial_bracket):
    """Save parallel_predictions output to a self-contained npz file.

    Embeds the initial bracket metadata (teams, seeds, regions) so that
    load_bracket_npz and compute_all_scores_vectorized don't need it passed
    separately.

    Usage
    -----
    bits_arr, probs_arr = parallel_predictions(...)
    save_predictions_npz(BRACKETS_NPZ.format(gender=gender), bits_arr, probs_arr, initial_bracket)
    """
    np.savez_compressed(
        path,
        bits=bits_arr,
        probs=probs_arr,
        slot_teams=initial_bracket['64'].values.astype(str),
        slot_seeds=initial_bracket['Seed'].values.astype(np.int8),
        slot_regions=initial_bracket['Region'].values.astype(str),
    )


def load_bracket_npz(brackets_npz, bracket_name):
    """Load a single bracket by name from an npz file and return a DataFrame.

    Parameters
    ----------
    brackets_npz : path to the .npz file written by save_predictions_npz
    bracket_name : string like 'bracket69025'

    Returns
    -------
    DataFrame with columns ['32', '16', '8', '4', '2', '1']

    Example
    -------
    bracket = load_bracket_npz(BRACKETS_NPZ.format(gender=gender), 'bracket69025')
    display(bracket)
    """
    idx  = int(re.search(r'\d+', bracket_name).group(0))
    data = np.load(brackets_npz)
    bracket = decode_bracket(data['bits'][idx], _reconstruct_initial_bracket(data))
    return bracket[['32', '16', '8', '4', '2', '1']]


def save_brackets_npz(brackets_npz, directory_name, folder, bracket_names):
    """Load named brackets from an npz file and save each as an HTML file.

    brackets_npz  : path to the .npz file produced by save_predictions_npz
    directory_name / folder: output directory components
    bracket_names : list of strings like ['bracket0', 'bracket42', ...]
    """
    directory = "{}/{}/".format(directory_name, folder)
    os.makedirs(directory, exist_ok=True)

    for name in bracket_names:
        load_bracket_npz(brackets_npz, name).to_html(
            "{}/{}.html".format(directory, name), border=0
        )


def read_scored_brackets(path):
    """Read a scored brackets file regardless of whether it is feather or parquet.

    Supports old .feather files from prior years and new .parquet files,
    so call sites don't need to know which format is on disk.

    Example
    -------
    df = read_scored_brackets(BRACKETS_PARQUET.format(gender=gender))
    df = read_scored_brackets("/path/to/2023/mens/brackets_2023.feather")
    """
    path = str(path)
    if path.endswith('.feather'):
        return pd.read_feather(path)
    elif path.endswith('.parquet'):
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unrecognised file extension: {path!r}. Expected .feather or .parquet.")
