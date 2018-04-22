import os
import numpy as np
import pandas as pd
from scipy.stats import norm
import timeit
import ijson.backends.yajl2_cffi as ijson
from ijson.common import ObjectBuilder
import json

def import_data(simple_filename):
    simple = pd.read_csv(simple_filename)

    order_of_regions = ['South', 'West', 'East', 'Midwest']  # This changes every year!
    order_of_teams = [1, 16, 8, 9, 5, 12, 4, 13, 6, 11, 3, 14, 7, 10, 2, 15]
    index = pd.MultiIndex.from_product([order_of_regions, order_of_teams], names=['Region', 'Seed'])

    initial_bracket = pd.DataFrame(data="", index=index, columns=['64', '32', '16', '8', '4', '2', '1'])
    for index, row in simple.iterrows():
        initial_bracket.loc[(row['team_region'], row['Seed']), '64'] = row['team_name']
    initial_bracket.reset_index(inplace=True)
    # initial_bracket['Team Num'] = range(len(initial_bracket))

    data = simple.set_index('team_name').reindex(initial_bracket['64']).reset_index()
    return initial_bracket, data


def compute_winner(pyths):
    A_em = pyths[0][2]
    A_t = pyths[0][3]
    B_em = pyths[1][2]
    B_t = pyths[1][3]
    point_diff = (A_em - B_em) * (A_t + B_t)/200
    # print point_diff
    d = norm(0,11)
    prob = d.cdf(point_diff)
    # print prob
    flip = np.random.uniform()
    if flip < prob:
        return pyths[0][0], pyths[0][1], prob
    else:
        return pyths[1][0], pyths[1][1], 1-prob


def compute_favorite(pyths):
    A_em = pyths[0][1]
    A_t = pyths[0][2]
    B_em = pyths[1][1]
    B_t = pyths[1][2]
    point_diff = (A_em - B_em) * (A_t + B_t)/200
    # print point_diff
    d = norm(0,11)
    prob = d.cdf(point_diff)
    print prob, pyths[0][0], pyths[1][0]
    if prob >= 0.5:
        return pyths[0][0], prob
    else:
        return pyths[1][0], 1-prob


def get_teams(top_team, gap, bracket, rounds, round, data):
    pyths = []
    for team in range(top_team, top_team + gap):
        team_name = bracket.loc[team, rounds[round]]
        if team_name != "":
            team_loc = data.iloc[team]
            pyths.append([team, team_name, team_loc['AdjustEM'], team_loc['AdjustT']])
    return pyths


def create_bracket(initial_bracket, data, start_time, sim):
    np.random.seed(sim)
    if sim % 10 == 0:
        stop_time = timeit.default_timer()
        print sim, ' ', stop_time-start_time
    bracket = initial_bracket.copy()
    rounds = ['64', '32', '16', '8', '4', '2', '1']
    probs = []
    for round in range(6):
        gap = 2**(round+1)
        for top_team in range(0,len(bracket),gap):
            pyths = get_teams(top_team, gap, bracket, rounds, round, data)
            # print pyths
            winner_num, winner_name, prob = compute_winner(pyths)
            probs.append(prob)
            # print winner
            bracket.loc[winner_num, rounds[round+1]] = winner_name

    full_prob = np.prod(probs)
    filename = 'bracket'+str(sim)

    # bracket_dict[filename]=bracket
    return filename, bracket, full_prob


def bracket_objects(file):
    key = '-'
    # for prefix, event, value in islice(ijson.parse(file), 10000):
    for prefix, event, value in ijson.parse(file):
        if prefix == '' and event == 'map_key':  # found new object at the root
            key = value  # mark the key value
            builder = ObjectBuilder()
        elif prefix.startswith(key):  # while at this key, build the object
            # if value == 'p' or value == 'pct':
            builder.event(event, value)
            if event == 'end_array':  # found the end of an object at the current key, yield
                # value_dict = builder.value
                # builder.value = {key: value_dict[key] for key in ['p', 'pct']}
                yield {key: builder.value}


def score_bracket(merged):
    total_score = 0
    potential_score = 0
    num_correct = []
    for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
        round_num = str(index + 2)
        binary_round = merged[round] != ''
        results_round = merged['rd' + round_num + '_win'] == 1
        potential_round = merged['rd' + round_num + '_win'] > 0
        score_vec = binary_round & results_round
        potential_vec = binary_round & potential_round
        pts_per_correct = 10 * (2 ** index)  # ESPN scoring
        score = score_vec.sum() * pts_per_correct
        pot_score = potential_vec.sum() * pts_per_correct
        total_score += score
        potential_score += pot_score
        num_correct.append(score_vec.sum())
    return total_score, potential_score, num_correct


def check_umbc(bracket):
    umbc = 0
    if bracket.loc[1, '32'] != "":
        umbc = 1
    return umbc


def num_16_seeds(merged):
    merged['seed'] = merged['team_seed'].apply(lambda x: int(filter(str.isdigit, x)))
    binary_round = merged['32']!=''
    seed_round = merged['seed']==16
    score_vec = binary_round & seed_round
    total_16 = score_vec.sum()
    return total_16


def check_longest_16(merged):
    longest_16 = 0
    for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
        binary_round = merged[round]!=''
        seed_round = merged['seed']==16
        score_vec = binary_round & seed_round
        pts_per_correct = 10*(2**index)      #ESPN scoring
        score = score_vec.sum() * pts_per_correct
        longest_16 += score
    return longest_16


def save_brackets(filename, folder, bracket_names):
    directory = os.path.dirname('Brackets_2018/' + folder + '/' + 'test')
    if not os.path.exists(directory):
        os.makedirs(directory)
    for obj in bracket_objects(open(filename)):
        bracket_name = obj.keys()[0]
        if bracket_name in bracket_names:
            bracket = pd.read_json(obj.values()[0][0])
            bracket = bracket[['32', '16', '8', '4', '2', '1']]
            bracket.to_html('Brackets_2018/' + folder + '/' + bracket_name + '.html', border=0)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)