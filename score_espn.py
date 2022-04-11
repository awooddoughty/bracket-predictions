import os
import pandas as pd
import numpy as np
import timeit
from multiprocessing import Pool
import ijson.backends.yajl2_cffi as ijson
from ijson.common import ObjectBuilder
from functools import partial
import pickle


def objects(file):
    key = '-'
    # for prefix, event, value in islice(ijson.parse(file), 10000):
    for prefix, event, value in ijson.parse(file):
        if prefix == '' and event == 'map_key':  # found new object at the root
            key = value  # mark the key value
            builder = ObjectBuilder()
        elif prefix.startswith(key):  # while at this key, build the object
            # if value == 'p' or value == 'pct':
            builder.event(event, value)
            if event == 'end_map':  # found the end of an object at the current key, yield
                value_dict = builder.value
                yield {key: builder.value}


def parse_espn(file, start_time):
    brackets = {}
    for obj in objects(open(file, 'rb')):
        brackets.update(obj)
    stop_time = timeit.default_timer()
    print(f"{file}: {stop_time - start_time}")
    return brackets


def create_actual_bracket(manual_bracket):
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
        brackets_df.to_feather(f"{new_name}.feather")
    else:
        brackets_df.to_feather(f"{output_filename}.feather")

