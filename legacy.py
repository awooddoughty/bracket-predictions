"""Legacy JSON-based helpers.

These functions support the old bracket JSON format used before 2026.
For current work use the npz/parquet format instead:
  - save_predictions_npz / load_bracket_npz  (bracket_io)
  - compute_all_scores_vectorized             (bracket_score / manual_scoring)
"""
import json
import os
import pandas as pd


def score_bracket(bracket, binary_bracket):
    """Compute actual and potential ESPN score for a single bracket."""
    total_score = 0
    potential_score = 0
    num_correct = []
    for index, round_ in enumerate(['32', '16', '8', '4', '2', '1']):
        round_num = str(index + 2)

        results_round  = bracket['rd' + round_num + '_win'] == 1
        potential_round = bracket['rd' + round_num + '_win'] > 0

        score_vec    = binary_bracket[round_] & results_round
        potential_vec = binary_bracket[round_] & potential_round

        pts = 10 * (2 ** index)
        total_score     += score_vec.sum()    * pts
        potential_score += potential_vec.sum() * pts
        num_correct.append(score_vec.sum())

    return total_score, potential_score, num_correct


def num_specific_seed_in_specific_round(binary_bracket, seed_bracket, seed_num, round_num):
    """How many teams with seed_num are predicted to reach round_num?"""
    return (binary_bracket[round_num] & seed_bracket[seed_num]).sum()


def bracket_objects(file):
    """Stream bracket objects from a large JSON file.

    Requires the ijson yajl2_cffi backend (brew install yajl).
    """
    try:
        import ijson.backends.yajl2_cffi as ijson_backend
        from ijson.common import ObjectBuilder
    except Exception as exc:
        raise ImportError(
            "bracket_objects requires the ijson yajl2_cffi backend. "
            "Use the npz format instead."
        ) from exc

    key = '-'
    for prefix, event, value in ijson_backend.parse(file):
        if prefix == '' and event == 'map_key':
            key = value
            builder = ObjectBuilder()
        elif prefix.startswith(key):
            builder.event(event, value)
            if event == 'end_array':
                yield {key: builder.value}


def save_brackets(brackets_json, directory_name, folder, bracket_names):
    """Retrieve named brackets from a JSON file and save each as an HTML file."""
    directory = os.path.dirname("{}/{}/".format(directory_name, folder))
    os.makedirs(directory, exist_ok=True)
    bracket_names = set(bracket_names)
    for obj in bracket_objects(open(brackets_json, 'rb')):
        bracket_name = list(obj)[0]
        if bracket_name in bracket_names:
            values  = list(obj.values())
            bracket = pd.read_json(values[0][0])
            bracket[['32', '16', '8', '4', '2', '1']].to_html(
                "{}/{}/{}.html".format(directory_name, folder, bracket_name), border=0
            )


class JSONEncoder(json.JSONEncoder):
    """Serialise pandas DataFrames inside bracket dicts to JSON."""
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)
