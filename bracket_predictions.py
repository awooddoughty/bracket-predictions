import json
from multiprocessing import Pool
from functools import partial
from bracket_functions import create_bracket, create_initial_bracket, JSONEncoder


def sequential_predictions(num_brackets, initial_bracket, data, sd_params, k, score_method, export_directory=None):
    """Sequential predictions to produce html for submitting brackets."""
    bracket_dict = {}
    for sim in range(num_brackets):
        filename, bracket, prob = create_bracket(initial_bracket, data, sd_params, k, score_method, sim)
        bracket_dict[filename] = [bracket, prob]
        if export_directory:
            bracket.drop(['64'], axis=1).to_html(f"{export_directory}/{filename}.html", border=0)
    return bracket_dict


def parallel_predictions(num_brackets, initial_bracket, data, sd_params, k, score_method):
    """Parallel predictions to produce json."""
    num_cores = 22

    pool = Pool(processes=num_cores)

    partial_bracket = partial(create_bracket, initial_bracket, data, sd_params, k, score_method)
    brackets = pool.map(partial_bracket, range(num_brackets))
    bracket_dict = {d[0]: [d[1], d[2]] for d in brackets}

    return bracket_dict
