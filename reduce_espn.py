import os
import pandas as pd
import timeit
from multiprocessing import Pool
import ijson.backends.yajl2_cffi as ijson
from ijson.common import ObjectBuilder

DIRECTORY = 'espn_brackets_2019/'
OUTPUT_FILENAME = 'espn_brackets_2019.feather'
NUM_CORES = 4


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
                builder.value = {key: value_dict[key] for key in ['p', 'pct']}
                yield {key: builder.value}


def parse_espn(file):
    brackets = {}
    for obj in objects(open(file, 'rb')):
        brackets.update(obj)
    return brackets


if __name__ == '__main__':
    start = timeit.default_timer()

    filenames = os.listdir(DIRECTORY)
    pool = Pool(processes=NUM_CORES)

    full_brackets = pool.map(parse_espn, filenames)
    # full_brackets = []
    # for filename in filenames[:3]:
    #     full_brackets.append(parse_espn(filename))

    brackets = {k: v for dct in full_brackets for k, v in dct.items()}

    brackets_df = pd.DataFrame.from_dict(brackets, orient='index')

    brackets_df['pct'] = pd.to_numeric(brackets_df['pct'])

    brackets_df.reset_index().to_feather(OUTPUT_FILENAME)

    stop = timeit.default_timer()
    print("Full Time: {}".format(stop - start))
