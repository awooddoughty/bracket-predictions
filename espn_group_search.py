import requests as req
import json
import pickle

GROUP_SEARCH_URL = ('http://fantasy.espncdn.com/tournament-challenge-bracket/2019/en/api/'
                    'findGroups?type_access=0&type_group=0&type_lock=0&type_dropWorst=0'
                    '&start={}&num={}')
OUTPUT_FILE = 'groups_2019.pkl'


def get_groups(start, num):
    response = req.get(GROUP_SEARCH_URL.format(start, num))
    data = json.loads(response.read())
    groups = map(lambda x: x['id'], data['g'])
    return groups


def scrape_groups():
    span_unit = 100000
    start = 0
    stop = 1700000
    full_groups = []
    for i in range(start, stop, span_unit):
        groups = get_groups(i, span_unit)
        print("{}: {}".format(i, len(groups)))
        full_groups.extend(groups)

    with open(OUTPUT_FILE, 'wb') as fp:
        pickle.dump(list(set(full_groups)), fp)
