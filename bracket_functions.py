import numpy as np
import pandas as pd
from scipy.stats import norm
import timeit
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


def create_bracket(initial_bracket, data, subtime_start, sim):
    if sim % 10 == 0:
        subtime_stop = timeit.default_timer()
        print sim, ' ', subtime_stop-subtime_start
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


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)