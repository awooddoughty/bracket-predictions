import os
import numpy as np
import pandas as pd
from scipy.stats import norm
import timeit
import ijson.backends.yajl2_cffi as ijson
from ijson.common import ObjectBuilder
import json
import re

DIRECTORY_NAME = 'Brackets_2018'
FILE_NAME = 'test'


def create_initial_bracket(simple_filename):
    """Convert team information into empty bracket."""
    simple = pd.read_csv(simple_filename)

    order_of_regions = ['East', 'West', 'South', 'Midwest']  # This changes every year!
    order_of_teams = [1, 16, 8, 9, 5, 12, 4, 13, 6, 11, 3, 14, 7, 10, 2, 15]
    index = pd.MultiIndex.from_product([order_of_regions, order_of_teams],
                                       names=['Region', 'Seed'])

    initial_bracket = pd.DataFrame(
        data="",
        index=index,
        columns=['64', '32', '16', '8', '4', '2', '1'],
    )
    for index, row in simple.iterrows():
        initial_bracket.loc[(row['team_region'], row['Seed']), '64'] = row['team_name']
    initial_bracket.reset_index(inplace=True)

    data = simple.set_index('team_name').reindex(initial_bracket['64']).reset_index()

    return initial_bracket, data


def compute_winner(pyths):
    """Compute the win probability and flip coin to determine winner."""
    A_team_id = pyths[0][0]
    A_team_name = pyths[0][1]
    A_em = pyths[0][2]
    A_t = pyths[0][3]
    B_team_id = pyths[1][0]
    B_team_name = pyths[1][1]
    B_em = pyths[1][2]
    B_t = pyths[1][3]

    point_diff = (A_em - B_em) * (A_t + B_t) / 200

    d = norm(0, 11)
    prob = d.cdf(point_diff)

    flip = np.random.uniform()
    if flip < prob:
        return A_team_id, A_team_name, prob
    else:
        return B_team_id, B_team_name, 1 - prob


def get_teams(top_team, gap, bracket, rounds, round, data):
    """Parse the bracket to get the teams that are playing each other."""
    pyths = []
    for team in range(top_team, top_team + gap):
        team_name = bracket.loc[team, rounds[round]]
        if team_name != "":
            team_loc = data.iloc[team]
            pyths.append([team, team_name, team_loc['AdjustEM'], team_loc['AdjustT']])
    return pyths


def create_bracket(initial_bracket, data, start_time, sim):
    """Fill out the bracket by iterating through each game."""
    np.random.seed(sim)
    if sim % 10000 == 0:
        stop_time = timeit.default_timer()
        print("{}: {}".format(sim, np.round(stop_time - start_time, 2)))

    bracket = initial_bracket.copy()
    rounds = ['64', '32', '16', '8', '4', '2', '1']
    probs = []
    for round in range(6):
        gap = 2**(round + 1)  # how far apart two teams could be for a given round
        for top_team in range(0, len(bracket), gap):
            pyths = get_teams(top_team, gap, bracket, rounds, round, data)
            winner_num, winner_name, prob = compute_winner(pyths)
            probs.append(prob)
            bracket.loc[winner_num, rounds[round + 1]] = winner_name  # write in the winner

    full_prob = np.prod(probs)  # compute the likelihood of the entire bracket
    filename = "bracket{}".format(sim)

    return filename, bracket, full_prob


def score_bracket(bracket):
    """Compute actual and potential score for a given bracket."""
    total_score = 0
    potential_score = 0
    num_correct = []
    for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
        round_num = str(index + 2)

        binary_round = bracket[round] != ''  # Who we predicted to win
        results_round = bracket['rd' + round_num + '_win'] == 1  # Who actually won
        potential_round = bracket['rd' + round_num + '_win'] > 0  # Who hasn't played yet

        score_vec = binary_round & results_round  # How many did we get right
        potential_vec = binary_round & potential_round  # How many could we still get right

        pts_per_correct = 10 * (2 ** index)  # ESPN scoring
        score_round = score_vec.sum() * pts_per_correct  # How many points did we get
        potential_score_round = potential_vec.sum() * pts_per_correct  # How many possible points

        total_score += score_round
        potential_score += potential_score_round
        num_correct.append(score_vec.sum())  # How many total games did we choose correctly
    return total_score, potential_score, num_correct


def num_16_seeds(merged):
    """How many 16 seeds won in the first round?"""
    merged['seed'] = merged['team_seed'].apply(lambda x: int(re.sub('\D', '', x)))
    binary_round = merged['32'] != ''
    seed_round = merged['seed'] == 16
    score_vec = binary_round & seed_round
    total_16 = score_vec.sum()
    return total_16


def check_longest_16(merged):
    """How far did a 16 seed go?"""
    longest_16 = 0
    for index, round in enumerate(['32', '16', '8', '4', '2', '1']):
        binary_round = merged[round] != ''
        seed_round = merged['seed'] == 16
        score_vec = binary_round & seed_round
        pts_per_correct = 10 * (2**index)  # ESPN scoring
        score = score_vec.sum() * pts_per_correct
        longest_16 += score
    return longest_16


def bracket_objects(file):
    """Parse the saved bracket json to be able to create an html bracket."""
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


def save_brackets(brackets_json, directory_name, folder, bracket_names):
    """Retrieve a set of named brackets and save them to html"""
    directory = os.path.dirname("{}/{}/".format(directory_name, folder))
    if not os.path.exists(directory):
        os.makedirs(directory)
    for obj in bracket_objects(open(brackets_json, 'rb')):
        bracket_name = bracket_name = list(obj)[0]
        if bracket_name in bracket_names:
            values = list(obj.values())
            bracket = pd.read_json(values[0][0])
            bracket = bracket[['32', '16', '8', '4', '2', '1']]
            bracket.to_html("{}/{}/{}.html".format(directory_name, folder, bracket_name), border=0)


class JSONEncoder(json.JSONEncoder):
    """Helper to export each bracket to json"""
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)
