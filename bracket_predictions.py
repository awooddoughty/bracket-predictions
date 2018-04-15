__author__ = 'Alex'
import numpy as np
import pandas as pd
import timeit
import json
from multiprocessing import Pool
from functools import partial
from bracket_functions import create_bracket, import_data, JSONEncoder

initial_bracket, data = import_data('simple_2018.csv')


def sequential_predictions(num_brackets):

    start = timeit.default_timer()
    bracket_dict = {}
    np.random.seed(22)
    for sim in range(num_brackets):
        filename, bracket, prob = create_bracket(initial_bracket, data, start, sim)
        # bracket.drop(['64'], axis=1).to_html('Brackets_test/'+filename+'.html', border=0)
        # print prob
        bracket_dict[filename] = [bracket, prob]

    stop = timeit.default_timer()
    print num_brackets, ' ', stop - start

    return bracket_dict


def parallel_predictions(num_brackets):
    num_cores = 22

    pool = Pool(processes=num_cores)

    start = timeit.default_timer()

    partial_bracket = partial(create_bracket, initial_bracket, data, start)
    brackets = pool.map(partial_bracket, range(num_brackets))
    bracket_dict = reduce(lambda r, d: r.update({d[0]: [d[1], d[2]]}) or r, brackets, {})

    stop = timeit.default_timer()
    print num_brackets, ' ', stop - start

    return bracket_dict



if __name__ == '__main__':
    bracket_dict = sequential_predictions(10)
    # bracket_dict = parallel_predictions(1000000)

    print bracket_dict
    # with open('brackets_2018.json', 'w') as fp:
    #     json.dump(bracket_dict, fp, sort_keys=True, indent=4, cls=JSONEncoder)