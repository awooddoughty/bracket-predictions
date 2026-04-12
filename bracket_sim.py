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

    n       = len(initial_bracket)
    rounds  = ['64', '32', '16', '8', '4', '2', '1']
    probs   = []
    bits    = np.uint64(0)
    bit_pos = np.uint64(0)

    # Pre-extract numpy arrays to avoid slow .loc indexing in the hot loop.
    if score_method == 'kenpom':
        adj_em    = data['AdjustEM'].values.copy()   # copy: may be updated in-place
        adj_t     = data['AdjustT'].values
        avg_tempo = float(adj_t.mean())
    elif score_method == '538':
        team_rating = data['team_rating'].values
    elif score_method == 'torvik':
        pyth = data['pyth'].values

    # slot[i] = current-round occupant's team name ("" if not yet/no longer competing).
    # Tracks bracket progress without touching the DataFrame in the read path.
    slot = initial_bracket['64'].values.copy()

    # Still build the result DataFrame (needed by sequential_predictions / HTML export).
    bracket = initial_bracket.copy()

    for round_ in range(6):
        gap       = 2 ** (round_ + 1)
        next_slot = np.full(n, "", dtype=object)

        for top_team in range(0, n, gap):
            # Fast numpy scan replaces the per-element bracket.loc reads in get_teams.
            teams = [t for t in range(top_team, top_team + gap) if slot[t] != ""]
            A_id, B_id = teams

            # Compute win probability using numpy array access (not data.loc).
            if score_method == 'kenpom':
                A_em_v = adj_em[A_id];  A_t_v = adj_t[A_id]
                B_em_v = adj_em[B_id];  B_t_v = adj_t[B_id]
                sd   = sd_params[0] + sd_params[1] * ((A_t_v + B_t_v) / 2 - avg_tempo) / avg_tempo
                prob = norm.cdf((A_em_v - B_em_v) * (A_t_v + B_t_v) / 200, scale=sd)
            elif score_method == '538':
                prob = 1 / (1 + 10 ** (-(team_rating[A_id] - team_rating[B_id]) / 400))
            elif score_method == 'torvik':
                pA = pyth[A_id];  pB = pyth[B_id]
                prob = (pA - pA * pB) / (pA + pB - 2 * pA * pB)

            if np.random.uniform() < prob:
                if score_method == 'kenpom' and k != 0:
                    adj_em[A_id] = A_em_v + k * (1 - prob)
                winner_num, winner_name, p = A_id, slot[A_id], prob
            else:
                if score_method == 'kenpom' and k != 0:
                    adj_em[B_id] = B_em_v + k * (1 - prob)
                winner_num, winner_name, p = B_id, slot[B_id], 1 - prob
                bits |= (np.uint64(1) << bit_pos)

            probs.append(p)
            next_slot[winner_num] = winner_name
            bracket.loc[winner_num, rounds[round_ + 1]] = winner_name
            bit_pos += np.uint64(1)

        slot = next_slot

    return f"bracket{sim}", bracket, np.prod(probs), bits


def _sim_bits_prob(initial_names, pre_extracted, sd_params, k, score_method, sim):
    """Lean simulation used by parallel_predictions — skips DataFrame construction.

    Parameters
    ----------
    initial_names   : np.ndarray of shape (64,) — team names from initial_bracket['64']
    pre_extracted   : dict of pre-extracted numpy arrays for the chosen score_method
    sd_params, k, score_method : same as create_bracket

    Returns
    -------
    (bits, prob) — same meaning as the last two values of create_bracket
    """
    np.random.seed(sim)

    n       = len(initial_names)
    probs   = []
    bits    = np.uint64(0)
    bit_pos = np.uint64(0)

    if score_method == 'kenpom':
        adj_em    = pre_extracted['adj_em'].copy()
        adj_t     = pre_extracted['adj_t']
        avg_tempo = pre_extracted['avg_tempo']
    elif score_method == '538':
        team_rating = pre_extracted['team_rating']
    elif score_method == 'torvik':
        pyth = pre_extracted['pyth']

    slot = initial_names.copy()

    for round_ in range(6):
        gap       = 2 ** (round_ + 1)
        next_slot = np.full(n, "", dtype=object)

        for top_team in range(0, n, gap):
            teams  = [t for t in range(top_team, top_team + gap) if slot[t] != ""]
            A_id, B_id = teams

            if score_method == 'kenpom':
                A_em_v = adj_em[A_id];  A_t_v = adj_t[A_id]
                B_em_v = adj_em[B_id];  B_t_v = adj_t[B_id]
                sd   = sd_params[0] + sd_params[1] * ((A_t_v + B_t_v) / 2 - avg_tempo) / avg_tempo
                prob = norm.cdf((A_em_v - B_em_v) * (A_t_v + B_t_v) / 200, scale=sd)
            elif score_method == '538':
                prob = 1 / (1 + 10 ** (-(team_rating[A_id] - team_rating[B_id]) / 400))
            elif score_method == 'torvik':
                pA = pyth[A_id];  pB = pyth[B_id]
                prob = (pA - pA * pB) / (pA + pB - 2 * pA * pB)

            if np.random.uniform() < prob:
                if score_method == 'kenpom' and k != 0:
                    adj_em[A_id] = A_em_v + k * (1 - prob)
                winner_num, p = A_id, prob
            else:
                if score_method == 'kenpom' and k != 0:
                    adj_em[B_id] = B_em_v + k * (1 - prob)
                winner_num, p = B_id, 1 - prob
                bits |= (np.uint64(1) << bit_pos)

            probs.append(p)
            next_slot[winner_num] = slot[winner_num]
            bit_pos += np.uint64(1)

        slot = next_slot

    return bits, np.prod(probs)


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
