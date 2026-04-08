import os
import pandas as pd
import numpy as np
import timeit
from multiprocessing import Pool
from functools import partial
import pickle


def objects(file):
    """Stream individual bracket dicts from a large ESPN JSON file.

    Requires the ijson yajl2_cffi backend (brew install yajl).

    Parameters
    ----------
    file : file-like object opened in binary mode

    Yields
    ------
    dict with one key (bracket id string) mapping to a bracket dict
    """
    try:
        import ijson.backends.yajl2_cffi as ijson_backend
        from ijson.common import ObjectBuilder
    except Exception as exc:
        raise ImportError(
            "score_espn.objects requires the ijson yajl2_cffi backend."
        ) from exc

    key = '-'
    for prefix, event, value in ijson_backend.parse(file):
        if prefix == '' and event == 'map_key':
            key = value
            builder = ObjectBuilder()
        elif prefix.startswith(key):
            builder.event(event, value)
            if event == 'end_map':
                yield {key: builder.value}


def parse_espn(file, start_time):
    """Parse all brackets from a single ESPN JSON file into a dict.

    Parameters
    ----------
    file       : path to the ESPN JSON file
    start_time : timeit timestamp for logging elapsed time after each file

    Returns
    -------
    dict {bracket_id: bracket_dict}
    """
    brackets = {}
    for obj in objects(open(file, 'rb')):
        brackets.update(obj)
    stop_time = timeit.default_timer()
    print(f"{file}: {stop_time - start_time}")
    return brackets


def create_actual_bracket(manual_bracket):
    """Build a DataFrame describing the actual tournament outcomes.

    Parameters
    ----------
    manual_bracket : list of 63 ints — the game-winner outcome for each of the
                     63 tournament games, in the same order used by score_espn_bracket.

    Returns
    -------
    DataFrame with columns: game_id (0..62), game_winner (int), round (1..6),
    point_value (10/20/40/80/160/320)
    """
    actual_bracket = pd.DataFrame({
        "game_id": range(63),
        "game_winner": manual_bracket,
    })
    actual_bracket["round"] = np.where(
        actual_bracket["game_id"] < 32, 1, np.where(
            actual_bracket["game_id"] < 48, 2, np.where(
                actual_bracket["game_id"] < 56, 3, np.where(
                    actual_bracket["game_id"] < 60, 4, np.where(
                        actual_bracket["game_id"] < 62, 5, 6)))))
    actual_bracket["point_value"] = 10 * (2 ** (actual_bracket["round"] - 1))
    return actual_bracket


def score_espn_bracket(bracket, actual_bracket):
    """Score a single ESPN bracket against the actual results.

    Parameters
    ----------
    bracket        : dict with keys 'id' and 'ps' (pipe-separated game picks string)
    actual_bracket : DataFrame from create_actual_bracket

    Returns
    -------
    dict {'id': bracket_id, 'score': int}
    """
    score = 0
    if bracket["ps"]:
        games = list(map(int, bracket["ps"].split("|")))
        score = (
            (
                actual_bracket["game_winner"].values == games
            ) * actual_bracket["point_value"].values
        ).sum()
    return {"id": bracket["id"], "score": score}


def initial_parse_espn(directory, output_filename, num_cores=2, num_files=None):
    """Parse all ESPN bracket JSON files in a directory and pickle the result.

    Call this once after downloading all ESPN bracket files.  The resulting
    pickle is then read by score_espn.

    Parameters
    ----------
    directory       : path to directory containing ESPN JSON files
    output_filename : base path for output; writes {output_filename}.pickle
    num_cores       : number of worker processes (default 2)
    num_files       : if set, only parse the first N files (useful for testing)
    """
    start_time = timeit.default_timer()
    filenames = [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if not file.startswith(".")
    ]
    if num_files:
        filenames = filenames[:num_files]
    
    pool = Pool(processes=num_cores)

    partial_parse = partial(parse_espn, start_time=start_time)
    full_brackets = pool.map(partial_parse, filenames)

    brackets = {k: v for dct in full_brackets for k, v in dct.items()}
    pickle.dump(brackets, open(f"{output_filename}.pickle", "wb"))


def score_espn(output_filename, manual_bracket, num_cores=2, new_name=None):
    """Score all ESPN brackets and write results to parquet.

    Reads the pickle produced by initial_parse_espn, scores every bracket
    against manual_bracket, and writes a parquet of (id, score) rows.

    Parameters
    ----------
    output_filename : base path used for both input pickle and output parquet
                      (unless new_name is given)
    manual_bracket  : list of 63 ints — actual game winners, passed to
                      create_actual_bracket
    num_cores       : number of worker processes (default 2)
    new_name        : if provided, write output to {new_name}.parquet instead
    """
    brackets = pickle.load(open(f"{output_filename}.pickle", "rb"))
    
    pool = Pool(processes=num_cores)
    
    actual_bracket = create_actual_bracket(manual_bracket=manual_bracket)
    
    partial_score = partial(
        score_espn_bracket,
        actual_bracket=actual_bracket,
    )
    scored_brackets = pool.map(partial_score, brackets.values())

    brackets_df = pd.DataFrame(scored_brackets)
    
    if new_name:
        brackets_df.to_parquet(f"{new_name}.parquet")
    else:
        brackets_df.to_parquet(f"{output_filename}.parquet")

