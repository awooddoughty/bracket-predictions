import os
import numpy as np
import pandas as pd
from scipy.stats import norm
import timeit
import ijson.backends.yajl2_cffi as ijson
from ijson.common import ObjectBuilder
import json
import re


def create_initial_bracket(simple_filename, order_of_regions):
    """Convert team information into empty bracket."""
    simple = pd.read_csv(simple_filename)

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
    
    
def compute_winner(data, teams, sd_params, k, avg_tempo, score_method):
    """Compute the win probability and flip coin to determine winner."""
    A_team_id, B_team_id = teams
    A_team_name = data.loc[A_team_id, '64']
    B_team_name = data.loc[B_team_id, '64']
    
    if score_method == "kenpom":
        A_em = data.loc[A_team_id, 'AdjustEM']
        A_t = data.loc[A_team_id, 'AdjustT']
        B_em = data.loc[B_team_id, 'AdjustEM']
        B_t = data.loc[B_team_id, 'AdjustT']
        sd = sd_params[0] + sd_params[1] * (((A_t + B_t) / 2) - avg_tempo) / avg_tempo
        d = norm(0, sd)
        diff = (A_em - B_em) * (A_t + B_t) / 200
        prob = d.cdf(diff)
    elif score_method == "538":
        A_538 = data.loc[A_team_id, 'team_rating']
        B_538 = data.loc[B_team_id, 'team_rating']
        diff = A_538 - B_538
#         c = 30.464  # pre-2025
        c = 1
        prob = 1 / (1 + 10**(-1 * diff * c / 400))
    else:
        raise Exception("Invalid score_method")

    flip = np.random.uniform()
    if flip < prob:
        if score_method == "kenpom":
            data.loc[A_team_id, 'AdjustEM'] = A_em + (k * (1 - prob))
        return A_team_id, A_team_name, prob
    else:
        if score_method == "kenpom":
            data.loc[B_team_id, 'AdjustEM'] = B_em + (k * (1 - prob))
        return B_team_id, B_team_name, 1 - prob


def get_teams(top_team, gap, bracket, rounds, round_, data):
    return [
        team
        for team in range(top_team, top_team + gap) if bracket.loc[team, rounds[round_]] != ""
    ]


def create_bracket(initial_bracket, data, sd_params, k, score_method, sim):
    """Fill out the bracket by iterating through each game."""
    np.random.seed(sim)

    bracket = initial_bracket.copy()
    data = data.copy()
    avg_tempo = data.get("AdjustT", np.array([np.nan])).mean()
    rounds = ['64', '32', '16', '8', '4', '2', '1']
    probs = []
    for round_ in range(6):
        gap = 2**(round_ + 1)  # how far apart two teams could be for a given round
        for top_team in range(0, len(bracket), gap):
            teams = get_teams(top_team, gap, bracket, rounds, round_, data)
            winner_num, winner_name, prob = compute_winner(data, teams, sd_params, k, avg_tempo, score_method)
            probs.append(prob)
            bracket.loc[winner_num, rounds[round_ + 1]] = winner_name  # write in the winner

    full_prob = np.prod(probs)  # compute the likelihood of the entire bracket
    filename = "bracket{}".format(sim)

    return filename, bracket, full_prob


def score_bracket(bracket, binary_bracket):
    """Compute actual and potential score for a given bracket."""
    total_score = 0
    potential_score = 0
    num_correct = []
    for index, round_ in enumerate(['32', '16', '8', '4', '2', '1']):
        round_num = str(index + 2)

        results_round = bracket['rd' + round_num + '_win'] == 1  # Who actually won
        potential_round = bracket['rd' + round_num + '_win'] > 0  # Who hasn't played yet

        score_vec = binary_bracket[round_] & results_round  # How many did we get right
        potential_vec = binary_bracket[round_] & potential_round  # How many could we still get right

        pts_per_correct = 10 * (2 ** index)  # ESPN scoring
        score_round = score_vec.sum() * pts_per_correct  # How many points did we get
        potential_score_round = potential_vec.sum() * pts_per_correct  # How many possible points

        total_score += score_round
        potential_score += potential_score_round
        num_correct.append(score_vec.sum())  # How many total games did we choose correctly
    return total_score, potential_score, num_correct


def num_specific_seed_in_specific_round(binary_bracket, seed_bracket, seed_num, round_num):
    """e.g. How many 16 seeds made it to the round of 32?"""
    return (binary_bracket[round_num] & seed_bracket[seed_num]).sum()


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
