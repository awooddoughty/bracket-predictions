import timeit
import json
from multiprocessing import Pool
from functools import partial
from bracket_functions import create_bracket, create_initial_bracket, JSONEncoder


def sequential_predictions(num_brackets, initial_bracket, data, sd_params, k):
    """Sequential predictions to produce html for submitting brackets."""
    start = timeit.default_timer()
    bracket_dict = {}
    for sim in range(num_brackets):
        filename, bracket, prob = create_bracket(initial_bracket, data, sd_params, k, start, sim)
        (bracket.drop(['64'], axis=1)
                .to_html("{}/{}.html".format(EXPORT_DIRECTORY, filename), border=0))
        bracket_dict[filename] = [bracket, prob]

    stop = timeit.default_timer()
    print("Completed {}: {}".format(num_brackets, round(stop - start, 2)))

    return bracket_dict


def parallel_predictions(num_brackets, initial_bracket, data, sd_params, k):
    """Parallel predictions to produce json."""
    num_cores = 22

    pool = Pool(processes=num_cores)

    start = timeit.default_timer()

    partial_bracket = partial(create_bracket, initial_bracket, data, sd_params, k, start)
    brackets = pool.map(partial_bracket, range(num_brackets))
    bracket_dict = {d[0]: [d[1], d[2]] for d in brackets}

    stop = timeit.default_timer()
    print("Completed {}: {}".format(num_brackets, round(stop - start, 2)))

    return bracket_dict
