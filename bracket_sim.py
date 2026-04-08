import numpy as np
import pandas as pd
from scipy.stats import norm


def create_initial_bracket(simple_filename, order_of_regions):
    """Load team ratings and build the empty bracket structure for one gender.

    Parameters
    ----------
    simple_filename : str
        Path to the simple_{YEAR}.csv file produced by create_original_brackets.ipynb.
        Must contain columns: team_name, team_region, Seed, and rating columns.
    order_of_regions : list[str]
        Region names in bracket order (changes each year, set in constants.py).

    Returns
    -------
    initial_bracket : DataFrame
        64-row DataFrame with Region, Seed, and a '64' column of team names.
        Round columns ('32', '16', '8', '4', '2', '1') are all empty strings.
    data : DataFrame
        Same 64 rows, indexed to match initial_bracket, with all rating columns
        attached.  Passed to create_bracket / compute_winner during simulation.
    """
    simple = pd.read_csv(simple_filename)

    order_of_teams = [1, 16, 8, 9, 5, 12, 4, 13, 6, 11, 3, 14, 7, 10, 2, 15]
    index = pd.MultiIndex.from_product([order_of_regions, order_of_teams],
                                       names=['Region', 'Seed'])

    initial_bracket = pd.DataFrame(
        data="",
        index=index,
        columns=['64', '32', '16', '8', '4', '2', '1'],
    )
    for _, row in simple.iterrows():
        initial_bracket.loc[(row['team_region'], row['Seed']), '64'] = row['team_name']
    initial_bracket.reset_index(inplace=True)

    data = simple.set_index('team_name').reindex(initial_bracket['64']).reset_index()

    return initial_bracket, data


def compute_winner(data, teams, sd_params, k, avg_tempo, score_method):
    """Compute win probability for a matchup and flip a coin to pick the winner.

    Parameters
    ----------
    data        : DataFrame indexed by row number, with team ratings columns.
                  Must contain 'AdjustEM'/'AdjustT' for kenpom, 'team_rating' for 538,
                  or 'pyth' for torvik.  Also must have a '64' column of team names.
    teams       : list of two row indices (ints) into data
    sd_params   : (intercept, slope) used only for score_method='kenpom'.
                  sd = intercept + slope * (avg_game_tempo - avg_tempo) / avg_tempo
    k           : Elo-style k-factor applied only when score_method='kenpom'.
                  Set to 0 to disable rating updates between rounds.
    avg_tempo   : mean AdjustT across all 64 teams; used only for kenpom sd calc.
    score_method: one of 'kenpom', '538', or 'torvik'

    Returns
    -------
    winner_id   : row index of the winning team in data
    winner_name : team name string from data['64']
    prob        : win probability of the winning team (always ≥ 0.5 is NOT guaranteed
                  — it is the probability of whichever team actually won)
    """
    A_team_id, B_team_id = teams

    if score_method == "kenpom":
        A_em = data.loc[A_team_id, 'AdjustEM']
        A_t  = data.loc[A_team_id, 'AdjustT']
        B_em = data.loc[B_team_id, 'AdjustEM']
        B_t  = data.loc[B_team_id, 'AdjustT']
        sd   = sd_params[0] + sd_params[1] * (((A_t + B_t) / 2) - avg_tempo) / avg_tempo
        prob = norm(0, sd).cdf((A_em - B_em) * (A_t + B_t) / 200)
    elif score_method == "538":
        prob = 1 / (1 + 10 ** (-1 * (data.loc[A_team_id, 'team_rating'] - data.loc[B_team_id, 'team_rating']) / 400))
    elif score_method == "torvik":
        A_pyth = data.loc[A_team_id, 'pyth']
        B_pyth = data.loc[B_team_id, 'pyth']
        prob   = (A_pyth - A_pyth * B_pyth) / (A_pyth + B_pyth - 2 * A_pyth * B_pyth)
    else:
        raise ValueError(f"Invalid score_method: {score_method!r}")

    if np.random.uniform() < prob:
        if score_method == "kenpom" and k != 0:
            data.loc[A_team_id, 'AdjustEM'] = A_em + (k * (1 - prob))
        return A_team_id, data.loc[A_team_id, '64'], prob
    else:
        if score_method == "kenpom" and k != 0:
            data.loc[B_team_id, 'AdjustEM'] = B_em + (k * (1 - prob))
        return B_team_id, data.loc[B_team_id, '64'], 1 - prob


def get_teams(top_team, gap, bracket, rounds, round_):
    """Return the two competing row indices for a given game slot.

    Scans the slice [top_team, top_team + gap) of bracket and returns the
    (at most two) rows that have a non-empty entry in the current round column.
    In round 0 (R64) both slots are always filled; in later rounds one slot
    may be empty if a team hasn't advanced yet.

    Parameters
    ----------
    top_team : int  — first row index of the pair's bracket region
    gap      : int  — number of rows spanned by this game (2, 4, 8, …)
    bracket  : DataFrame with round columns indexed 0..63
    rounds   : list of column names ['64', '32', '16', '8', '4', '2', '1']
    round_   : int  — current round index (0–5)

    Returns
    -------
    list of two row indices
    """
    return [
        team
        for team in range(top_team, top_team + gap)
        if bracket.loc[team, rounds[round_]] != ""
    ]


def create_bracket(initial_bracket, data, sd_params, k, score_method, sim):
    """Simulate a full bracket for a given random seed.

    Returns
    -------
    filename : str
        'bracket{sim}'
    bracket : DataFrame
        Completed bracket with winners filled in for each round column.
    full_prob : float
        Joint probability of the 63 game outcomes.
    bits : np.uint64
        63-bit encoding of game outcomes (bit i=0 → teams[0] won, bit i=1 → teams[1] won).
        Use save_predictions_npz to persist this efficiently.
    """
    np.random.seed(sim)

    bracket   = initial_bracket.copy()
    data      = data.copy()
    avg_tempo = data.get("AdjustT", np.array([np.nan])).mean()
    rounds    = ['64', '32', '16', '8', '4', '2', '1']
    probs     = []
    bits      = np.uint64(0)
    bit_pos   = np.uint64(0)

    for round_ in range(6):
        gap = 2 ** (round_ + 1)
        for top_team in range(0, len(bracket), gap):
            teams = get_teams(top_team, gap, bracket, rounds, round_)
            winner_num, winner_name, prob = compute_winner(data, teams, sd_params, k, avg_tempo, score_method)
            probs.append(prob)
            bracket.loc[winner_num, rounds[round_ + 1]] = winner_name
            if winner_num == teams[1]:
                bits |= (np.uint64(1) << bit_pos)
            bit_pos += np.uint64(1)

    return f"bracket{sim}", bracket, np.prod(probs), bits


def decode_bracket(bits, initial_bracket):
    """Reconstruct a bracket DataFrame from its 63-bit encoding.

    Parameters
    ----------
    bits : np.uint64
        Value returned by create_bracket (or loaded from an npz file).
    initial_bracket : DataFrame
        Base bracket with teams in the '64' column.

    Returns
    -------
    Completed bracket DataFrame, identical to what create_bracket would have produced.
    """
    bracket = initial_bracket.copy()
    rounds  = ['64', '32', '16', '8', '4', '2', '1']
    bit_pos = np.uint64(0)

    for round_ in range(6):
        gap = 2 ** (round_ + 1)
        for top_team in range(0, len(bracket), gap):
            teams      = get_teams(top_team, gap, bracket, rounds, round_)
            winner_num = teams[int((bits >> bit_pos) & np.uint64(1))]
            bracket.loc[winner_num, rounds[round_ + 1]] = bracket.loc[winner_num, '64']
            bit_pos += np.uint64(1)

    return bracket
