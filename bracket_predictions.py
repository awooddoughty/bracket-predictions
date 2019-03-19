import timeit
import json
from multiprocessing import Pool
from functools import partial
from bracket_functions import create_bracket, create_initial_bracket, JSONEncoder

DATA_FILE_NAME = 'simple_2019.csv'
EXPORT_JSON = 'brackets_test.json'
EXPORT_DIRECTORY = 'Brackets_test'


def sequential_predictions(num_brackets):
    """Sequential predictions to produce html for submitting brackets."""
    start = timeit.default_timer()
    bracket_dict = {}
    for sim in range(num_brackets):
        filename, bracket, prob = create_bracket(initial_bracket, data, start, sim)
        (bracket.drop(['64'], axis=1)
                .to_html("{}/{}.html".format(EXPORT_DIRECTORY, filename), border=0))
        bracket_dict[filename] = [bracket, prob]

    stop = timeit.default_timer()
    print num_brackets, ' ', stop - start

    return bracket_dict


def parallel_predictions(num_brackets):
    """Parallel predictions to produce json."""
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
    initial_bracket, data = create_initial_bracket(DATA_FILE_NAME)

    bracket_dict = sequential_predictions(10)
    # bracket_dict = parallel_predictions(10)

    with open(EXPORT_JSON, 'w') as fp:
        json.dump(bracket_dict, fp, sort_keys=True, indent=4, cls=JSONEncoder)
