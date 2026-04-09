"""Streamlit web app for NCAA bracket predictions.

Run with:
    streamlit run app.py
"""
import os
import re
import sys
import warnings
import importlib

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

warnings.filterwarnings("ignore", category=UserWarning, module="fuzzywuzzy")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import constants
from scrape_functions import (
    scrape_espn_data, parse_bracket_data, match_team_names,
    scrape_kenpom, scrape_torvik,
)
from bracket_sim import create_initial_bracket
from bracket_io import _reconstruct_initial_bracket, save_predictions_npz, read_scored_brackets
from bracket_score import compute_all_scores_vectorized
from bracket_predictions import parallel_predictions
from bracket_sim import decode_bracket


# ── Path helpers ──────────────────────────────────────────────────────────────

def _p(year, gender):
    """Shorthand for constants.get_paths(year, gender)."""
    return constants.get_paths(year, gender)


def _available_years():
    """Years that have ESPN IDs configured (for scraping / generation)."""
    return sorted(constants.ESPN_IDS.keys(), reverse=True)


def _has_order_of_regions(year):
    return isinstance(constants.ORDER_OF_REGIONS, dict) and year == constants.YEAR


# ── Name-override helpers ─────────────────────────────────────────────────────

def _overrides_dict_from_df(df: pd.DataFrame) -> dict:
    return {r: e for r, e in zip(df["ratings_name"], df["espn_name"]) if r and e}


def _write_back_overrides(gender: str, new_overrides: dict) -> None:
    """Surgically patch the NAME_OVERRIDES[gender] sub-dict in constants.py."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "constants.py")
    source = open(path).read()
    items = ",\n        ".join(f"{k!r}: {v!r}" for k, v in new_overrides.items())
    replacement = f'"{gender}": {{\n        {items},\n    }}'
    new_source = re.sub(
        rf'"{gender}"\s*:\s*\{{[^}}]*\}}',
        replacement,
        source,
        flags=re.DOTALL,
    )
    if new_source == source:
        st.warning("Pattern not found in constants.py — NAME_OVERRIDES not updated.")
        return
    with open(path, "w") as f:
        f.write(new_source)
    importlib.reload(constants)


def _init_overrides_df(gender: str) -> pd.DataFrame:
    overrides = constants.NAME_OVERRIDES.get(gender, {})
    return pd.DataFrame(list(overrides.items()), columns=["ratings_name", "espn_name"])


# ── Bracket HTML generator ────────────────────────────────────────────────────

_BRACKET_CSS = """
<style>
body { margin: 0; }
.bkt { display:flex; gap:3px; font-size:11px; font-family:monospace;
       background:#f5f5f5; padding:6px; }
.bkt-left, .bkt-right { display:flex; flex-direction:column; gap:3px; }
.bkt-center { display:flex; flex-direction:column; justify-content:center;
              align-items:stretch; gap:6px; padding:0 6px; min-width:130px; }
table.reg { border-collapse:collapse; }
th.rh { background:#1a237e; color:#fff; text-align:center;
        padding:3px 6px; font-size:11px; }
th.rnd { background:#e8eaf6; color:#444; text-align:center;
         padding:1px 3px; font-size:9px; }
td.t { height:22px; min-width:118px; max-width:118px;
       padding:1px 3px; border:1px solid #ccc;
       vertical-align:middle; overflow:hidden; white-space:nowrap; }
td.t.w { background:#e8f5e9; font-weight:bold; }
td.t.l { background:#fafafa; color:#bbb; }
td.t.u { background:#fff; }
.s { color:#999; font-size:9px; min-width:17px; display:inline-block; }
.ff-box { border:1px solid #bbb; background:#fff; padding:3px 5px; }
.ff-title { font-size:9px; color:#888; text-align:center; margin-bottom:2px;
            text-transform:uppercase; letter-spacing:.5px; }
.ff-t { height:22px; min-width:120px; padding:1px 3px; border:1px solid #ccc;
        display:flex; align-items:center; overflow:hidden; white-space:nowrap; }
.ff-t.w { background:#e8f5e9; font-weight:bold; }
.ff-t.l { background:#fafafa; color:#bbb; }
.ff-t.u { background:#fff; }
.champ-box { border:2px solid #1a237e; background:#e8eaf6;
             padding:6px 8px; text-align:center; }
.champ-lbl { font-size:10px; font-weight:bold; color:#1a237e; }
.champ-nm  { font-size:14px; font-weight:bold; color:#1a237e; margin-top:3px; }
</style>
"""


def _bracket_html(bracket_df):
    """Generate full-bracket HTML from a decoded bracket DataFrame."""

    regions_ordered = list(dict.fromkeys(bracket_df["Region"].tolist()))
    if len(regions_ordered) != 4:
        return "<p>Unexpected region count.</p>"

    left_r  = regions_ordered[:2]
    right_r = regions_ordered[2:]

    # ── helpers ────────────────────────────────────────────────────────────────

    def _short(name, n=13):
        return name[:n] + "…" if len(name) > n else name

    def _t_cell(seed, name, won, rowspan=1):
        cls = "w" if won else "l"
        rs  = f' rowspan="{rowspan}"' if rowspan > 1 else ""
        return (f'<td class="t {cls}"{rs}>'
                f'<span class="s">{seed}</span>{_short(name)}</td>')

    def _empty(rowspan=1):
        rs = f' rowspan="{rowspan}"' if rowspan > 1 else ""
        return f'<td class="t u"{rs}></td>'

    def _find(rdf, start, size, col, next_col):
        """Return (seed, name, won_next) for the winner in the group, or None."""
        group = rdf.iloc[start:start + size]
        wins  = group[group[col] != ""]
        if wins.empty:
            return None
        w = wins.iloc[0]
        return int(w["Seed"]), str(w["64"]), (w[next_col] != "" if next_col else False)

    # ── region tables ──────────────────────────────────────────────────────────

    def _region_left(name):
        rdf  = bracket_df[bracket_df["Region"] == name].reset_index(drop=True)
        rows = []
        for i in range(16):
            row  = rdf.iloc[i]
            won  = row["32"] != ""
            cells = [_t_cell(int(row["Seed"]), str(row["64"]), won)]
            if i % 2 == 0:
                r = _find(rdf, i, 2, "32", "16")
                cells.append(_t_cell(*r[:2], r[2], 2) if r else _empty(2))
            if i % 4 == 0:
                r = _find(rdf, i, 4, "16", "8")
                cells.append(_t_cell(*r[:2], r[2], 4) if r else _empty(4))
            if i % 8 == 0:
                r = _find(rdf, i, 8, "8", "4")
                cells.append(_t_cell(*r[:2], r[2], 8) if r else _empty(8))
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return (f'<table class="reg">'
                f'<thead><tr><th class="rh" colspan="4">{name}</th></tr>'
                f'<tr><th class="rnd">R64</th><th class="rnd">R32</th>'
                f'<th class="rnd">S16</th><th class="rnd">E8</th></tr></thead>'
                f'<tbody>{"".join(rows)}</tbody></table>')

    def _region_right(name):
        rdf  = bracket_df[bracket_df["Region"] == name].reset_index(drop=True)
        rows = []
        for i in range(16):
            row  = rdf.iloc[i]
            won  = row["32"] != ""
            cells = []
            if i % 8 == 0:
                r = _find(rdf, i, 8, "8", "4")
                cells.append(_t_cell(*r[:2], r[2], 8) if r else _empty(8))
            if i % 4 == 0:
                r = _find(rdf, i, 4, "16", "8")
                cells.append(_t_cell(*r[:2], r[2], 4) if r else _empty(4))
            if i % 2 == 0:
                r = _find(rdf, i, 2, "32", "16")
                cells.append(_t_cell(*r[:2], r[2], 2) if r else _empty(2))
            cells.append(_t_cell(int(row["Seed"]), str(row["64"]), won))
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return (f'<table class="reg">'
                f'<thead><tr><th class="rh" colspan="4">{name}</th></tr>'
                f'<tr><th class="rnd">E8</th><th class="rnd">S16</th>'
                f'<th class="rnd">R32</th><th class="rnd">R64</th></tr></thead>'
                f'<tbody>{"".join(rows)}</tbody></table>')

    # ── center: FF + Championship ──────────────────────────────────────────────

    def _ff_box(r1_name, r2_name):
        def _ff_team(rname):
            ff = bracket_df[bracket_df["Region"] == rname]
            ff = ff[ff["4"] != ""]
            if ff.empty:
                return '<div class="ff-t u">TBD</div>'
            t   = ff.iloc[0]
            cls = "w" if t["2"] != "" else "l"
            return (f'<div class="ff-t {cls}">'
                    f'<span class="s">{int(t["Seed"])}</span>{_short(t["64"])}</div>')
        return (f'<div class="ff-box"><div class="ff-title">Final Four</div>'
                f'{_ff_team(r1_name)}{_ff_team(r2_name)}</div>')

    def _champ_box():
        champs   = bracket_df[bracket_df["2"] != ""]
        champion = bracket_df[bracket_df["1"] != ""]
        teams_html = ""
        for _, t in champs.iterrows():
            cls = "w" if t["1"] != "" else "l"
            teams_html += (f'<div class="ff-t {cls}">'
                           f'<span class="s">{int(t["Seed"])}</span>'
                           f'{_short(t["64"])}</div>')
        if champion.empty:
            champ_html = ""
        else:
            ch = champion.iloc[0]
            champ_html = (f'<div class="champ-box">'
                          f'<div class="champ-lbl">Champion</div>'
                          f'<div class="champ-nm">({int(ch["Seed"])}) {_short(ch["64"], 16)}'
                          f'</div></div>')
        return (f'<div class="ff-box"><div class="ff-title">Championship</div>'
                f'{teams_html}</div>{champ_html}')

    # ── assemble ───────────────────────────────────────────────────────────────
    left_html  = "".join(_region_left(r)  for r in left_r)
    right_html = "".join(_region_right(r) for r in right_r)
    center_html = (_ff_box(left_r[0], left_r[1])
                   + _champ_box()
                   + _ff_box(right_r[0], right_r[1]))

    return (f"{_BRACKET_CSS}"
            f'<div class="bkt">'
            f'<div class="bkt-left">{left_html}</div>'
            f'<div class="bkt-center">{center_html}</div>'
            f'<div class="bkt-right">{right_html}</div>'
            f'</div>')


# ── Tab renderers ─────────────────────────────────────────────────────────────

def _render_bracket_creation(year, gender):
    st.header("Bracket Creation")

    paths = _p(year, gender)

    # Reset state when year or gender changes
    state_key = f"t1_{year}_{gender}"
    if st.session_state.get("t1_state_key") != state_key:
        for k in ["t1_espn_df", "t1_ratings_df", "t1_mapping",
                  "t1_unmatched_ratings", "t1_unmatched_espn"]:
            st.session_state[k] = None
        st.session_state.t1_overrides_df     = _init_overrides_df(gender)
        st.session_state.t1_ratings_unsaved  = False
        st.session_state.t1_state_key        = state_key

    # ── Step 1: ESPN teams ────────────────────────────────────────────────────
    st.subheader("Step 1 — ESPN team names")

    if year not in constants.ESPN_IDS:
        st.warning(f"No ESPN ID configured for {year}. Add it to ESPN_IDS in constants.py.")
    else:
        if st.button("Scrape ESPN data", key="btn_scrape"):
            with st.spinner("Fetching from ESPN…"):
                try:
                    res = scrape_espn_data(id_=constants.ESPN_IDS[year][gender])
                    df  = parse_bracket_data(res)[["team_name", "region_id", "seed"]]
                    df["team_region"] = df["region_id"].apply(
                        lambda x: dict(enumerate(constants.ORDER_OF_REGIONS[gender]))[x - 1]
                    )
                    st.session_state.t1_espn_df = df
                    st.session_state.t1_mapping = None
                    st.success(f"Scraped {len(df)} teams from ESPN")
                except Exception as e:
                    st.error(f"ESPN scrape failed: {e}")

    if st.session_state.t1_espn_df is not None:
        with st.expander(f"View {len(st.session_state.t1_espn_df)} scraped teams"):
            st.dataframe(st.session_state.t1_espn_df, use_container_width=True, hide_index=True)

    # ── Step 2: Ratings ───────────────────────────────────────────────────────
    st.subheader("Step 2 — Ratings file")

    ratings_source = "KenPom" if gender == "mens" else "Torvik"
    ratings_file   = paths.kenpom if gender == "mens" else paths.torvik
    scrape_fn      = scrape_kenpom if gender == "mens" else lambda y: scrape_torvik(y, gender)

    col_scrape, col_existing, col_upload = st.columns(3)

    with col_scrape:
        if st.button(f"Scrape {ratings_source}", key="btn_scrape_ratings"):
            with st.spinner(f"Scraping {ratings_source}… (opens headless browser)"):
                try:
                    df = scrape_fn(year)
                    st.session_state.t1_ratings_df = df
                    st.session_state.t1_ratings_unsaved = True
                    st.session_state.t1_mapping = None
                    st.success(f"Scraped {len(df)} teams — review below then save")
                except Exception as e:
                    st.error(f"Scrape failed: {e}")

    with col_existing:
        if os.path.exists(ratings_file):
            if st.button(f"Use existing {ratings_source} CSV", key="btn_existing_ratings"):
                df = pd.read_csv(ratings_file)
                st.session_state.t1_ratings_df = df
                st.session_state.t1_ratings_unsaved = False
                st.session_state.t1_mapping = None
                st.success(f"Loaded {len(df)} teams from {ratings_file}")
        else:
            st.caption(f"No existing file:\n`{ratings_file}`")

    with col_upload:
        uploaded = st.file_uploader(f"Or upload {ratings_source} CSV",
                                    type="csv", key=f"upload_{year}_{gender}")
        if uploaded is not None:
            df = pd.read_csv(uploaded)
            required = {"Team", "AdjustEM", "AdjustT"} if gender == "mens" else {"Team", "pyth"}
            missing  = required - set(df.columns)
            if missing:
                st.error(f"Missing columns: {missing}")
            else:
                st.session_state.t1_ratings_df = df
                st.session_state.t1_ratings_unsaved = True
                st.session_state.t1_mapping = None
                st.success(f"Loaded {len(df)} teams from upload — review below then save")

    if st.session_state.t1_ratings_df is not None:
        ratings_df_preview = st.session_state.t1_ratings_df
        col_preview, col_save_ratings = st.columns([3, 1])
        with col_preview:
            with st.expander(f"View {len(ratings_df_preview)} ratings rows"):
                st.dataframe(ratings_df_preview, use_container_width=True, hide_index=True)
        with col_save_ratings:
            unsaved = st.session_state.get("t1_ratings_unsaved", False)
            label   = f"💾 Save {ratings_source} CSV" if unsaved else f"✓ {ratings_source} CSV saved"
            if st.button(label, key="btn_save_ratings", disabled=not unsaved):
                try:
                    os.makedirs(paths.dir, exist_ok=True)
                    ratings_df_preview.to_csv(ratings_file, index=False)
                    st.session_state.t1_ratings_unsaved = False
                    st.success(f"Saved → `{ratings_file}`")
                except Exception as e:
                    st.error(f"Save failed: {e}")

    # ── Step 3: Name matching ─────────────────────────────────────────────────
    if st.session_state.t1_espn_df is None or st.session_state.t1_ratings_df is None:
        return

    st.subheader("Step 3 — Name overrides")
    st.caption("Add rows here for names that fuzzy matching gets wrong. "
               "Keys = ratings names (KenPom/Torvik), Values = ESPN names.")

    edited = st.data_editor(
        st.session_state.t1_overrides_df,
        num_rows="dynamic",
        column_config={
            "ratings_name": st.column_config.TextColumn("Ratings name"),
            "espn_name":    st.column_config.TextColumn("ESPN name"),
        },
        use_container_width=True,
        key="overrides_editor",
    )
    st.session_state.t1_overrides_df = edited

    col_match, col_save = st.columns([1, 1])
    with col_match:
        if st.button("Run / re-run matching", key="btn_match"):
            overrides = _overrides_dict_from_df(st.session_state.t1_overrides_df)
            mapping, unmatched_r, unmatched_e = match_team_names(
                st.session_state.t1_ratings_df,
                st.session_state.t1_espn_df["team_name"].tolist(),
                overrides=overrides,
            )
            st.session_state.t1_mapping           = mapping
            st.session_state.t1_unmatched_ratings = unmatched_r
            st.session_state.t1_unmatched_espn    = unmatched_e

    if st.session_state.t1_mapping is None:
        return

    # ── Matching results ──────────────────────────────────────────────────────
    unmatched_r = st.session_state.t1_unmatched_ratings or []
    unmatched_e = st.session_state.t1_unmatched_espn    or []
    mapping     = st.session_state.t1_mapping

    c1, c2, c3 = st.columns(3)
    c1.metric("Matched",          len(mapping))
    c2.metric("Unmatched ratings", len(unmatched_r))
    c3.metric("Unmatched ESPN",    len(unmatched_e))

    if unmatched_r:
        st.warning(f"Unmatched ratings (excluded from CSV): {unmatched_r}")
    if unmatched_e:
        st.warning(f"Unmatched ESPN teams: {unmatched_e}")

    with col_save:
        if st.button("Save CSV + update constants.py", key="btn_save"):
            try:
                ratings_df = st.session_state.t1_ratings_df.copy()
                ratings_df["team_name"] = ratings_df["Team"].map(mapping)
                team_info  = st.session_state.t1_espn_df
                keep_cols  = (
                    ["team_region", "Seed", "team_name", "AdjustEM", "AdjustT"]
                    if gender == "mens"
                    else ["team_region", "Seed", "team_name", "pyth"]
                )
                simple = ratings_df.merge(
                    team_info,
                    left_on=["team_name", "Seed"],
                    right_on=["team_name", "seed"],
                )[keep_cols]

                if len(simple) != 64:
                    st.error(
                        f"Expected 64 teams after merge, got {len(simple)}. "
                        "Fix name/seed mismatches in the overrides above."
                    )
                else:
                    os.makedirs(paths.dir, exist_ok=True)
                    simple.to_csv(paths.simple, index=False)
                    _write_back_overrides(
                        gender, _overrides_dict_from_df(st.session_state.t1_overrides_df)
                    )
                    st.success(f"Saved {len(simple)} teams → {paths.simple}")
                    with st.expander("View saved data"):
                        st.dataframe(simple, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Save failed: {e}")


def _render_generate(year, gender):
    st.header("Generate Brackets")

    paths = _p(year, gender)

    if not os.path.exists(paths.simple):
        st.error(f"Simple CSV not found:\n`{paths.simple}`\n\nComplete Bracket Creation first.")
        return
    st.success(f"Simple CSV: `{paths.simple}`")

    if not _has_order_of_regions(year):
        st.error(f"ORDER_OF_REGIONS is not configured for {year}. "
                 "Update constants.py to generate brackets for this year.")
        return

    num_brackets = st.number_input(
        "Number of brackets",
        min_value=1_000, max_value=2_000_000, value=100_000, step=10_000,
        key="t2_num",
    )

    # Defaults: kenpom for mens, torvik for womens; sd_params=(11,0), k=0
    score_method = "kenpom" if gender == "mens" else "torvik"
    sd_params    = (11, 0)
    k            = 0
    st.caption(f"Score method: **{score_method}** · sd_params: {sd_params} · k: {k}")

    if st.button("Generate brackets", key="btn_generate"):
        os.makedirs(paths.dir, exist_ok=True)
        with st.spinner(f"Simulating {num_brackets:,} brackets…"):
            try:
                initial_bracket, data = create_initial_bracket(
                    paths.simple, constants.ORDER_OF_REGIONS[gender]
                )
                bits_arr, probs_arr = parallel_predictions(
                    num_brackets, initial_bracket, data,
                    sd_params=sd_params, k=k, score_method=score_method,
                )
                save_predictions_npz(paths.npz, bits_arr, probs_arr, initial_bracket)
                msg = (f"Saved {num_brackets:,} brackets → `{paths.npz}`")
                st.session_state.t2_status = msg
                st.success(msg)
            except Exception as e:
                st.error(f"Generation failed: {e}")
    elif st.session_state.get("t2_status"):
        st.info(st.session_state.t2_status)

    if os.path.exists(paths.npz):
        size_mb = os.path.getsize(paths.npz) / 1e6
        st.caption(f"Existing NPZ: `{paths.npz}` ({size_mb:.1f} MB)")


def _render_score(year, gender):
    st.header("Score & Summarize")

    paths = _p(year, gender)

    # ── Score brackets ────────────────────────────────────────────────────────
    with st.expander("Score brackets (run once after tournament results are available)",
                     expanded=not os.path.exists(paths.parquet)):
        if not os.path.exists(paths.npz):
            st.warning(f"NPZ file not found: `{paths.npz}` — generate brackets first.")
        elif year not in constants.ESPN_IDS:
            st.warning(f"No ESPN ID for {year} — cannot scrape results.")
        else:
            st.caption("Scrapes live tournament results from ESPN then scores all brackets.")
            if st.button("Score brackets", key="btn_score"):
                with st.spinner("Fetching ESPN results…"):
                    try:
                        results = parse_bracket_data(
                            scrape_espn_data(id_=constants.ESPN_IDS[year][gender])
                        )
                    except Exception as e:
                        st.error(f"ESPN scrape failed: {e}")
                        return
                with st.spinner("Scoring brackets (vectorized)…"):
                    try:
                        compute_all_scores_vectorized(results, paths.npz, paths.parquet)
                        st.success(f"Scores saved → `{paths.parquet}`")
                    except Exception as e:
                        st.error(f"Scoring failed: {e}")

    # ── Load scored table ─────────────────────────────────────────────────────
    if not os.path.exists(paths.parquet):
        st.info("No scored brackets file yet. Score brackets above.")
        return

    try:
        df = read_scored_brackets(paths.parquet)
    except Exception as e:
        st.error(f"Failed to load brackets: {e}")
        return

    st.caption(f"Loaded {len(df):,} brackets from `{paths.parquet}`")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total brackets", f"{len(df):,}")
    m2.metric("Max score",       int(df["score"].max()))
    m3.metric("Mean score",      f"{df['score'].mean():.1f}")
    m4.metric("Median score",    int(df["score"].median()))

    with st.expander("Filters", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            score_range = st.slider(
                "Score", int(df["score"].min()), int(df["score"].max()),
                (int(df["score"].min()), int(df["score"].max())), key="t3_score",
            )
        with fc2:
            pot_range = st.slider(
                "Potential score", int(df["potential_score"].min()), int(df["potential_score"].max()),
                (int(df["potential_score"].min()), int(df["potential_score"].max())), key="t3_pot",
            )
        with fc3:
            name_filter = st.text_input("Search name", key="t3_name")

    default_cols = [c for c in ["name","score","potential_score","32","16","8","4","2","1","likelihood"]
                    if c in df.columns]
    extra_cols   = [c for c in df.columns if c not in default_cols]
    chosen_extra = st.multiselect("Add columns", extra_cols, default=[], key="t3_extra")
    display_cols = default_cols + chosen_extra

    sort_by = st.selectbox(
        "Sort by",
        [c for c in ["score","potential_score","likelihood"] if c in df.columns],
        key="t3_sort",
    )

    mask = df["score"].between(*score_range) & df["potential_score"].between(*pot_range)
    if name_filter:
        mask &= df["name"].str.contains(name_filter, case=False, na=False)
    filtered = df.loc[mask, display_cols].sort_values(sort_by, ascending=False)

    col_config = {
        "name":            st.column_config.TextColumn("Bracket"),
        "score":           st.column_config.NumberColumn("Score",     format="%d"),
        "potential_score": st.column_config.NumberColumn("Potential", format="%d"),
        "likelihood":      st.column_config.NumberColumn("Likelihood", format="%.2e"),
    }
    for r in ["32","16","8","4","2","1"]:
        if r in display_cols:
            col_config[r] = st.column_config.NumberColumn(f"R{r}", format="%d")

    st.dataframe(filtered, use_container_width=True, height=500,
                 column_config=col_config, hide_index=True)
    st.caption(f"Showing {len(filtered):,} of {len(df):,} brackets")

    st.subheader("Top 10 brackets")
    st.dataframe(df.nlargest(10, "score")[default_cols], use_container_width=True,
                 hide_index=True, column_config=col_config)

    st.subheader("Score distribution")
    hist = (df["score"].value_counts().sort_index()
            .rename_axis("score").reset_index(name="count"))
    st.bar_chart(hist.set_index("score"))


def _render_visualize(year, gender):
    st.header("Visualize Bracket")

    paths = _p(year, gender)

    if not os.path.exists(paths.npz):
        st.warning(f"NPZ file not found: `{paths.npz}` — generate brackets first.")
        return

    try:
        npz_data = np.load(paths.npz)
        n_brackets = len(npz_data["bits"])
    except Exception as e:
        st.error(f"Failed to load NPZ: {e}")
        return

    st.caption(f"Loaded `{paths.npz}` — {n_brackets:,} brackets available")

    col_num, col_prob = st.columns([2, 2])
    with col_num:
        bracket_num = st.number_input(
            "Bracket number", min_value=0, max_value=n_brackets - 1,
            value=0, step=1, key="t4_bracket_num",
        )
    with col_prob:
        prob = float(npz_data["probs"][bracket_num])
        st.metric("Bracket probability", f"{prob:.4e}")

    # Load scored brackets to show this bracket's score if available
    if os.path.exists(paths.parquet):
        try:
            scores_df = read_scored_brackets(paths.parquet)
            row = scores_df[scores_df["name"] == f"bracket{bracket_num}"]
            if not row.empty:
                r = row.iloc[0]
                sc1, sc2, sc3 = st.columns(3)
                sc1.metric("Score",           int(r["score"]))
                sc2.metric("Potential score", int(r["potential_score"]))
                sc3.metric("Percentile", f"{(scores_df['score'] < r['score']).mean()*100:.1f}%")
        except Exception:
            pass

    # Decode and render
    try:
        initial_bracket = _reconstruct_initial_bracket(npz_data)
        bracket_df      = decode_bracket(npz_data["bits"][bracket_num], initial_bracket)
        html = _bracket_html(bracket_df)
        components.html(html, height=980, scrolling=True)
    except Exception as e:
        st.error(f"Failed to render bracket: {e}")


# ── Page layout ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Bracket Predictions", layout="wide")

for _k, _v in {
    "t1_espn_df":          None,
    "t1_ratings_df":       None,
    "t1_mapping":          None,
    "t1_unmatched_ratings": None,
    "t1_unmatched_espn":   None,
    "t1_overrides_df":     None,
    "t1_state_key":        None,
    "t1_ratings_unsaved":  False,
    "t2_status":           "",
}.items():
    st.session_state.setdefault(_k, _v)

st.title("NCAA Bracket Predictions")

# ── Global controls (year + gender) ──────────────────────────────────────────
top_col1, top_col2 = st.columns([1, 3])
with top_col1:
    year_options = _available_years()
    year = st.selectbox("Year", year_options, key="year",
                        index=year_options.index(constants.YEAR) if constants.YEAR in year_options else 0)
with top_col2:
    gender = st.radio("Gender", ["mens", "womens"], horizontal=True, key="gender")

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Bracket Creation",
    "🎲 Generate Brackets",
    "📊 Score & Summarize",
    "🗓 Visualize Bracket",
])

with tab1:
    _render_bracket_creation(year, gender)

with tab2:
    _render_generate(year, gender)

with tab3:
    _render_score(year, gender)

with tab4:
    _render_visualize(year, gender)
