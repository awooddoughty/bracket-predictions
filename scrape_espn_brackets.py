
import os
import requests
import json
from multiprocessing import Pool
import timeit
from functools import partial


base = 'http://g.espncdn.com/tournament-challenge-bracket/2018/en/api/'


def by_group(group_id, start_time):
    if group_id % 10000 == 0:
        stop_time = timeit.default_timer()
        print group_id, ' ', stop_time - start_time
    url = base + 'group?length=10000&groupID=' + str(group_id)
    d = {}
    start = 0
    while True:
        try:
            resp = requests.get(url + '&start=' + str(start))
        except requests.exceptions.RequestException as e:
            print(group_id, e)
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
            print(group_id, e)
            continue
    # print group_id, len(d)
    return d


def groups_by_id_robust(full_start, full_stop, save_gap):
    start_time = timeit.default_timer()
    i = 2
    for start in range(full_start, full_stop, save_gap):
        entries = {}
        stop = start + save_gap
        stop = full_stop if stop > full_stop else stop
        groups = range(start, stop)
        pool = Pool(processes=20)
        partial_group = partial(by_group, start_time=start_time)
        results = pool.map(partial_group, groups)
        pool.close()
        pool.join()
        for r in results:
            entries.update(r)
        json.dump(entries, open('espn_brackets_{}.json'.format(i), 'w'))
        i += 1


def groups_by_id():
    groups = range(1500000, 2800000)
    pool = Pool(processes=20)
    start = timeit.default_timer()
    partial_group = partial(by_group, start_time=start)
    results = pool.map(partial_group, groups)
    pool.close()
    pool.join()
    entries = {}
    for r in results:
        entries.update(r)
    return entries


def by_entry(entry_id, start_time):
    if entry_id % 10 == 0:
        stop_time = timeit.default_timer()
        print entry_id, ' ', stop_time - start_time

    url = base + 'entry?entryID=' + str(entry_id)
    try:
        resp = requests.get(url)

        data = json.loads(resp.text)

        # print(data)
        return {data['e']['id']:data['e']}

    except Exception:
        return {}


def entries_by_id():
    entries = range(1, 1000)
    pool = Pool(processes=1)
    start = timeit.default_timer()
    partial_entry = partial(by_entry, start_time=start)
    results = pool.map(partial_entry, entries)
    pool.close()
    pool.join()
    entries = {}
    for r in results:
        entries.update(r)
    return entries


if __name__ == '__main__':
    start_time = timeit.default_timer()

    out = 'espn_brackets2.json'

    old_entries = {}
    # try:
    #     old_entries = json.load(open(out))
    # except Exception:
    #     pass

    groups_by_id_robust(1600000, 2800000, 100000)
    # new_entries = groups_by_id_robust(1600000, 2800000, 100000)
    # new_entries = groups_by_id_robust(1500000, 1600000, 10000)

    # old_entries.update(new_entries)
    # # entries = entries_by_id()
    # json.dump(old_entries, open(out, 'w'))

    stop_time = timeit.default_timer()
    print "Full Time: {} Hours".format((stop_time - start_time)/60/60)
