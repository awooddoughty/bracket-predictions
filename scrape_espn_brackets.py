import requests
import json
from multiprocessing import Pool
import timeit
from functools import partial
import pickle


API_BASE = 'http://fantasy.espn.com/tournament-challenge-bracket/2019/en/api/'

GROUP_FILE = 'groups_2019.pkl'
OUTPUT_DIRECTORY = 'espn_brackets_2019/'
OUTPUT_FILE_STUB = OUTPUT_DIRECTORY + 'espn_brackets_{}.json'
SAVE_GAP = 3
NUM_CORES = 1


def by_group(group_id, start_time):
    if group_id % 10000 == 0:
        stop_time = timeit.default_timer()
        print("{}: {}".format(group_id, stop_time - start_time))
    url = API_BASE + 'group?length=10000&groupID=' + str(group_id)
    d = {}
    start = 0
    while True:
        try:
            resp = requests.get(url + '&start=' + str(start))
        except requests.exceptions.RequestException as e:
            print("{}: {}".format(group_id, e))
            continue
        try:
            data = json.loads(resp.text)
            if 'g' not in data or 'e' not in data['g']:
                break
            else:
                ent = data['g']['e']
                start += len(ent)
                for e in ent:
                    d[e['id']] = e
        except Exception, e:
            print("{}: {}".format(group_id, e))
            continue
    return d


def groups_from_list(file, save_gap, processes=4):
    start_time = timeit.default_timer()

    groups = pickle.load(open(file))

    i = 0
    for start in range(0, len(groups), save_gap):
        entries = {}
        stop = start + save_gap
        stop = len(groups) if stop > len(groups) else stop
        partial_groups = groups[start:stop]
        pool = Pool(processes=processes)
        partial_group = partial(by_group, start_time=start_time)
        results = pool.map(partial_group, partial_groups)
        pool.close()
        pool.join()
        for r in results:
            entries.update(r)
        json.dump(entries, open(OUTPUT_FILE_STUB.format(i), 'w'))
        i += 1


if __name__ == '__main__':
    start_time = timeit.default_timer()

    groups_from_list(GROUP_FILE, SAVE_GAP, processes=NUM_CORES)

    stop_time = timeit.default_timer()
    print("Full Time: {} Hours".format((stop_time - start_time) / 60 / 60))
