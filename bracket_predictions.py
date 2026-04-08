import numpy as np
from multiprocessing import Pool
from functools import partial

from bracket_sim import create_bracket, create_initial_bracket
from bracket_io import save_predictions_npz
from legacy import JSONEncoder


def sequential_predictions(num_brackets, initial_bracket, data, sd_params, k, score_method, export_directory=None):
    """Simulate brackets sequentially and optionally export each as HTML.

    Use this when you need the full bracket DataFrames (e.g. for HTML export).
    For large runs, use parallel_predictions instead — it is much faster and
    returns the compact npz-compatible arrays.

    Parameters
    ----------
    num_brackets     : number of brackets to simulate
    initial_bracket  : DataFrame from create_initial_bracket
    data             : ratings DataFrame from create_initial_bracket
    sd_params        : passed through to compute_winner
    k                : Elo k-factor passed through to compute_winner
    score_method     : 'kenpom', '538', or 'torvik'
    export_directory : if provided, each bracket is written to
                       {export_directory}/bracket{n}.html

    Returns
    -------
    dict mapping bracket name → [bracket_df, prob]
    """
    bracket_dict = {}
    for sim in range(num_brackets):
        filename, bracket, prob, _bits = create_bracket(initial_bracket, data, sd_params, k, score_method, sim)
        bracket_dict[filename] = [bracket, prob]
        if export_directory:
            bracket.drop(['64'], axis=1).to_html(f"{export_directory}/{filename}.html", border=0)
    return bracket_dict


def parallel_predictions(num_brackets, initial_bracket, data, sd_params, k, score_method):
    """Parallel predictions returning compact numpy arrays.

    Returns
    -------
    bits_arr : np.ndarray, shape (num_brackets,), dtype uint64
        63-bit encoding of each bracket's game outcomes.
    probs_arr : np.ndarray, shape (num_brackets,), dtype float64
        Joint probability of each bracket outcome.

    Save to disk with save_predictions_npz.
    """
    pool = Pool(processes=None)
    results = pool.map(
        partial(create_bracket, initial_bracket, data, sd_params, k, score_method),
        range(num_brackets),
    )

    bits_arr  = np.array([r[3] for r in results], dtype=np.uint64)
    probs_arr = np.array([r[2] for r in results], dtype=np.float64)

    return bits_arr, probs_arr
