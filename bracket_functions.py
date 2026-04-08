"""Backward-compatibility shim.

All functionality has moved to:
  bracket_sim  — simulation (create_initial_bracket, create_bracket, decode_bracket, ...)
  bracket_io   — file I/O  (save_predictions_npz, load_bracket_npz, read_scored_brackets, ...)
  legacy       — JSON-path helpers (bracket_objects, save_brackets, JSONEncoder, ...)

Existing notebook imports from bracket_functions will continue to work unchanged.
"""
from bracket_sim import (
    create_initial_bracket,
    compute_winner,
    get_teams,
    create_bracket,
    decode_bracket,
)
from bracket_io import (
    _reconstruct_initial_bracket,
    save_predictions_npz,
    load_bracket_npz,
    save_brackets_npz,
    read_scored_brackets,
)
from legacy import (
    score_bracket,
    num_specific_seed_in_specific_round,
    bracket_objects,
    save_brackets,
    JSONEncoder,
)
