__author__ = 'Alex'

import numpy as np
import pandas as pd
import timeit
import json
from multiprocessing import Pool
from functools import partial
import re
import ijson.backends.yajl2_cffi as ijson
# import ijson
from itertools import islice
import seaborn as sns
import matplotlib.pyplot as plt
import os
from ijson.common import ObjectBuilder


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

filename_stub = 'espn_brackets_'
filenames = [filename_stub+str(i)+'.json' for i in range(14)]

num_cores = 22
pool = Pool(processes=num_cores)

start = timeit.default_timer()

full_brackets = pool.map(parse_espn, filenames)
# full_brackets = []
# for filename in filenames[:3]:
#     full_brackets.append(parse_espn(filename))


brackets = reduce(lambda r, d: r.update(d) or r, full_brackets, {})

brackets_df = pd.DataFrame.from_dict(brackets, orient='index')

brackets_df['pct'] = pd.to_numeric(brackets_df['pct'])

brackets_df.reset_index().to_feather('espn_brackets_2018.feather')

stop = timeit.default_timer()
print stop - start



