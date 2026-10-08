"""
Transformation and integration stage (README section 6.8).

Built step by step:
    Step 1 - Spotify: one row per track + track/artist and track/genre relations   (done)
    Step 2 - Genre families (dim_genre)                                             (done)
    Step 3 - Grammy: awards per credited artist, category type, unknown member      (done)
    Step 4 - Integration: conformed dim_artist and integration contract             (done)
    Step 5 - Segments (candidate / consolidated)                                    (done)
    Step 6 - Assemble the dimensional model with surrogate keys + persist outputs   (done)

Every transformation is a documented engineering rule, not a fix to make a validation pass.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

AUDIO_FEATURES = ["energy", "valence", "danceability", "acousticness"]


# ---------------------------------------------------------------------------
# Shared rule: artist name normalization (integration key)
# ---------------------------------------------------------------------------
def normalize_artist_name(value) -> str | None:
    """
    Comparison key for artist names across sources.

    Rule: Unicode NFKD -> remove accents -> lower case -> collapse inner spaces -> trim.
    'Beyoncé ' -> 'beyonce' ; 'The  Beatles' -> 'the beatles'.
    Returns None when nothing meaningful is left (the name cannot be used as a key).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text or None


# ---------------------------------------------------------------------------
# Step 1 - Spotify
# ---------------------------------------------------------------------------
def prepare_spotify(raw: pd.DataFrame) -> dict:
    """
    From the raw Spotify batch (one row per track x genre) build:

    tracks        one row per track_id with popularity, audio features, is_explicit
    track_artists one row per (track_id, artist) after splitting `artists` by ';'
    track_genres  one row per (track_id, track_genre)
    metrics       counts that document the effect of each rule
    """
    df = raw.copy()
    rows_in = len(df)

    # Rule S1 - grain: one row per track. Duplicates come from one row per genre.
    # When copies disagree on popularity, keep the row with the MAXIMUM popularity
    # (deterministic; avoids flagging an artist as low-popularity because of a stale value).
    df = df.sort_values(["track_id", "popularity"], ascending=[True, False], kind="mergesort")
    popularity_variants = df.groupby("track_id")["popularity"].nunique()
    tracks = df.drop_duplicates("track_id", keep="first").copy()

    # Rule S2 - explicit (bool) -> is_explicit (0/1) so its mean is the % of explicit tracks.
    tracks["is_explicit"] = tracks["explicit"].astype(bool).astype(int)

    tracks = tracks[["track_id", "popularity", *AUDIO_FEATURES, "is_explicit"]].reset_index(drop=True)
    tracks["popularity"] = tracks["popularity"].astype(int)

    # Rule S3 - multi-artist tracks: split `artists` by ';' into one row per artist.
    # Tracks without artists are kept in `tracks` but get no artist relation.
    artist_rows = (
        df.drop_duplicates("track_id")[["track_id", "artists"]]
        .dropna(subset=["artists"])
        .assign(artist_name=lambda d: d["artists"].str.split(";"))
        .explode("artist_name")
    )
    artist_rows["artist_name"] = artist_rows["artist_name"].str.strip()
    artist_rows["artist_norm"] = artist_rows["artist_name"].map(normalize_artist_name)
    unusable_names = int(artist_rows["artist_norm"].isna().sum())
    track_artists = (
        artist_rows.dropna(subset=["artist_norm"])
        .drop_duplicates(["track_id", "artist_norm"])[["track_id", "artist_name", "artist_norm"]]
        .reset_index(drop=True)
    )

    # Rule S4 - multi-genre tracks: keep every (track, genre) pair for the genre bridge.
    track_genres = (
        df.dropna(subset=["track_genre"])
        .drop_duplicates(["track_id", "track_genre"])[["track_id", "track_genre"]]
        .reset_index(drop=True)
    )

    tracks_with_artist = track_artists["track_id"].nunique()
    metrics = {
        "rows_in": rows_in,
        "tracks_out": len(tracks),
        "duplicate_rows_collapsed": rows_in - len(tracks),
        "tracks_with_popularity_conflict": int((popularity_variants > 1).sum()),
        "tracks_without_artist": len(tracks) - tracks_with_artist,
        "unusable_artist_names": unusable_names,
        "track_artist_pairs": len(track_artists),
        "multi_artist_tracks": int((track_artists.groupby("track_id").size() > 1).sum()),
        "distinct_spotify_artists": int(track_artists["artist_norm"].nunique()),
        "track_genre_pairs": len(track_genres),
        "multi_genre_tracks": int((track_genres.groupby("track_id").size() > 1).sum()),
    }
    return {"tracks": tracks, "track_artists": track_artists,
            "track_genres": track_genres, "metrics": metrics}


# ---------------------------------------------------------------------------
# Step 2 - Genre families (dim_genre)
# ---------------------------------------------------------------------------
# Rule G1: the 114 Spotify genre labels are grouped into 12 families so AR2 can
# answer "in which genre families are candidates concentrated?" at a decision level.
_FAMILY_GENRES = {
    "Pop": ["pop", "power-pop", "synth-pop", "indie-pop", "pop-film", "k-pop", "j-pop",
            "j-idol", "j-dance", "cantopop", "mandopop"],
    "Rock": ["rock", "alt-rock", "alternative", "hard-rock", "psych-rock", "punk", "punk-rock",
             "grunge", "emo", "indie", "j-rock", "rock-n-roll", "rockabilly", "garage", "goth"],
    "Metal": ["metal", "heavy-metal", "black-metal", "death-metal", "metalcore", "grindcore",
              "hardcore", "industrial"],
    "Electrónica": ["edm", "electronic", "electro", "house", "deep-house", "chicago-house",
                    "progressive-house", "techno", "detroit-techno", "minimal-techno", "trance",
                    "dubstep", "drum-and-bass", "breakbeat", "hardstyle", "idm", "trip-hop",
                    "club", "dance", "disco"],
    "Hip-hop, R&B y Soul": ["hip-hop", "r-n-b", "soul", "funk", "gospel", "groove"],
    "Latina": ["latin", "latino", "reggaeton", "salsa", "samba", "tango", "sertanejo", "pagode",
               "forro", "mpb", "brazil", "spanish"],
    "Reggae y Afro": ["reggae", "dancehall", "ska", "dub", "afrobeat"],
    "Jazz y Blues": ["jazz", "blues"],
    "Country y Folk": ["country", "folk", "bluegrass", "honky-tonk", "singer-songwriter",
                       "songwriter", "acoustic", "guitar"],
    "Clásica e Instrumental": ["classical", "opera", "piano", "new-age", "ambient"],
    "Regional / Internacional": ["british", "french", "german", "swedish", "iranian", "turkish",
                                 "indian", "malay", "world-music"],
    "Contexto y Ánimo": ["happy", "sad", "chill", "sleep", "study", "party", "romance",
                         "children", "kids", "disney", "comedy", "show-tunes", "anime"],
}
GENRE_FAMILY_MAP = {g: family for family, genres in _FAMILY_GENRES.items() for g in genres}
GENRE_FAMILIES = list(_FAMILY_GENRES)
UNMAPPED_FAMILY = "Sin clasificar"  # deliberately NOT in GENRE_FAMILIES -> DQ16 blocks the load


def build_dim_genre(track_genres: pd.DataFrame) -> dict:
    """
    One row per genre label with its family.

    Exception handling: a label that is not in the catalog (e.g. a new genre in a future batch)
    is marked 'Sin clasificar'. The transformation does not hide it; DQ16 (critical) rejects it
    in validate_prepared, so the catalog must be reviewed before loading.
    """
    genres = sorted(track_genres["track_genre"].dropna().unique())
    dim_genre = pd.DataFrame({"genre": genres})
    dim_genre["genre_family"] = dim_genre["genre"].map(GENRE_FAMILY_MAP).fillna(UNMAPPED_FAMILY)

    unmapped = dim_genre.loc[dim_genre["genre_family"] == UNMAPPED_FAMILY, "genre"].tolist()
    metrics = {
        "genres": len(dim_genre),
        "families": int(dim_genre["genre_family"].nunique()),
        "unmapped_genres": unmapped,
        "genres_per_family": dim_genre["genre_family"].value_counts().to_dict(),
    }
    return {"dim_genre": dim_genre, "metrics": metrics}


# ---------------------------------------------------------------------------
# Step 3 - Grammy
# ---------------------------------------------------------------------------
# Unambiguous collaboration markers. '&', ',' and 'and' are NOT split here because they are
# part of many real act names (Simon & Garfunkel, Earth, Wind & Fire); they are resolved in
# step 4 against the Spotify catalog (match the full name first, split only if needed).
FEATURING_PATTERN = re.compile(r"\s+(?:featuring|feat\.?|ft\.?)\s+", flags=re.IGNORECASE)


def _clean_label(value) -> str | None:
    """Trim and collapse inner spaces; keep the original casing for display."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def prepare_grammys(raw: pd.DataFrame) -> dict:
    """
    From the raw Grammy batch (one row per award) build:

    award_credits  one row per (award, credited artist); awards without artist keep one row
                   with is_unknown_artist = True (they will point to the "Desconocido" member)
    categories     one row per category with category_type (artística / técnica)
    years          one row per ceremony year with its decade
    metrics        counts that document the effect of each rule
    """
    df = raw.copy()
    rows_in = len(df)

    # Rule GR1 - consistent labels: trim / collapse spaces in category and artist.
    df["category"] = df["category"].map(_clean_label)
    df["artist"] = df["artist"].map(_clean_label)
    df["year"] = df["year"].astype(int)

    # Rule GR2 - category type. A category whose awards NEVER carry an artist in the batch is
    # 'técnica' (engineering, production, album notes...): it awards non-performers (PR01).
    category_has_artist = df.groupby("category")["artist"].apply(lambda s: s.notna().any())
    categories = category_has_artist.rename("has_artist").reset_index()
    categories["category_type"] = categories["has_artist"].map({True: "artística", False: "técnica"})
    categories = categories.rename(columns={"category": "category_name"})[["category_name", "category_type"]]

    # Rule GR3 - collaborations: split the credited text on featuring markers only.
    # 'A Featuring B' -> one row for A and one for B; each receives the full award.
    df["is_unknown_artist"] = df["artist"].isna()
    df["artist_name"] = df["artist"].map(
        lambda a: [p.strip() for p in FEATURING_PATTERN.split(a) if p.strip()] if isinstance(a, str) else [None]
    )
    featuring_awards = int((df["artist_name"].map(len) > 1).sum())
    credits = df.explode("artist_name")
    credits["artist_norm"] = credits["artist_name"].map(normalize_artist_name)

    # Rule GR4 - awards without artist are kept (never dropped) and flagged; in step 4 they
    # point to the "Desconocido" member so totals reconcile with the source (4,810 awards).
    award_credits = credits[[
        "source_row_number", "year", "category", "artist_name", "artist_norm", "is_unknown_artist",
    ]].drop_duplicates(["source_row_number", "artist_norm"]).reset_index(drop=True)

    # Rule GR5 - ceremony years with their decade (dim_year).
    years = pd.DataFrame({"year": sorted(df["year"].unique())})
    years["decade"] = (years["year"] // 10 * 10).astype(str) + "s"

    metrics = {
        "rows_in": rows_in,
        "awards_out": int(award_credits["source_row_number"].nunique()),
        "award_credit_rows": len(award_credits),
        "awards_with_featuring": featuring_awards,
        "awards_without_artist": int(df["is_unknown_artist"].sum()),
        "categories": len(categories),
        "technical_categories": int((categories["category_type"] == "técnica").sum()),
        "artistic_categories": int((categories["category_type"] == "artística").sum()),
        "distinct_grammy_artists": int(award_credits["artist_norm"].nunique()),
        "years": len(years),
    }
    return {"award_credits": award_credits, "categories": categories,
            "years": years, "metrics": metrics}


# ---------------------------------------------------------------------------
# Step 4 - Integration (conformed dim_artist)
# ---------------------------------------------------------------------------
UNKNOWN_ARTIST_KEY = 0
UNKNOWN_ARTIST_NORM = "<desconocido>"
UNKNOWN_ARTIST_NAME = "Desconocido"
# Credits that do not name a performer (compilations). They cannot be reissue candidates,
# so they are routed to the "Desconocido" (non-attributable) member instead of being
# treated as an artist missing from Spotify.
NON_ATTRIBUTABLE_NAMES = {"(various artists)", "various artists", "various"}
# Ambiguous separators, used ONLY when the full name is not in Spotify (see resolve rule).
COMPOSITE_PATTERN = re.compile(r"\s*,\s*|\s+&\s+|\s+and\s+|\s+with\s+", flags=re.IGNORECASE)


def _article_variant(norm: str) -> str:
    """'the beatles' <-> 'beatles' (leading article is often written inconsistently)."""
    return norm[4:] if norm.startswith("the ") else f"the {norm}"


def _resolve_grammy_name(norm: str, spotify_keys: set) -> tuple[list[str], str]:
    """
    Integration rule I2 - resolve one normalized Grammy credit against the Spotify catalog.
    Order matters (most reliable first):
      0. non-attributable credit ('Various Artists') -> [UNKNOWN_ARTIST_NORM]
      1. exact normalized match                       -> [norm]
      2. leading-article variant ('the ...')          -> [variant]
      3. composite name split on , / & / and / with,
         accepted ONLY if EVERY part exists in Spotify -> [part1, part2, ...]
      4. otherwise keep the full name, unmatched      -> [norm]  (Grammy-only artist)
    Never invents artists: an ambiguous name is split only when the catalog confirms all parts.
    """
    if norm in NON_ATTRIBUTABLE_NAMES:
        return [UNKNOWN_ARTIST_NORM], "non_attributable"
    if norm in spotify_keys:
        return [norm], "exact"
    variant = _article_variant(norm)
    if variant in spotify_keys:
        return [variant], "article_variant"
    parts = [normalize_artist_name(p) for p in COMPOSITE_PATTERN.split(norm)]
    parts = [p for p in dict.fromkeys(parts) if p]
    if len(parts) > 1 and all(p in spotify_keys for p in parts):
        return parts, "composite_split"
    return [norm], "unmatched"


def integrate(spotify: dict, grammy: dict) -> dict:
    """
    Build the conformed artist dimension shared by both fact tables.

    Integration key: artist_norm (normalize_artist_name) on both sources.
    Returns:
      award_artists  one row per (award, resolved artist); unknown awards -> UNKNOWN_ARTIST_NORM
      dim_artist     one row per artist_norm with artist_key and integration attributes
      unmatched      Grammy artists not found in Spotify (evidence of coverage limits)
      metrics        integration-contract evidence
    """
    track_artists = spotify["track_artists"]
    tracks = spotify["tracks"]
    credits = grammy["award_credits"]
    spotify_keys = set(track_artists["artist_norm"])

    # --- I2: resolve each distinct Grammy name once (deterministic) -----------------------
    grammy_norms = sorted(credits.loc[~credits["is_unknown_artist"], "artist_norm"].dropna().unique())
    resolution = []
    for norm in grammy_norms:
        resolved, method = _resolve_grammy_name(norm, spotify_keys)
        resolution += [{"artist_norm": norm, "resolved_norm": r, "match_method": method} for r in resolved]
    resolution = pd.DataFrame(resolution, columns=["artist_norm", "resolved_norm", "match_method"])

    known = credits[~credits["is_unknown_artist"]].merge(resolution, on="artist_norm", how="left")
    unknown = credits[credits["is_unknown_artist"]].assign(
        resolved_norm=UNKNOWN_ARTIST_NORM, match_method="unknown_member")
    award_artists = (
        pd.concat([known, unknown], ignore_index=True)
        .drop_duplicates(["source_row_number", "resolved_norm"])
        [["source_row_number", "year", "category", "resolved_norm", "match_method"]]
        .rename(columns={"resolved_norm": "artist_norm"})
        .sort_values(["source_row_number", "artist_norm"]).reset_index(drop=True)
    )

    # --- I3: display name = most frequent original spelling (Spotify spelling preferred) ----
    sp_names = track_artists.groupby("artist_norm")["artist_name"].agg(lambda s: s.value_counts().index[0])
    gr_names = (credits.dropna(subset=["artist_norm"]).groupby("artist_norm")["artist_name"]
                .agg(lambda s: s.value_counts().index[0]))

    # --- per-artist integration attributes ---------------------------------------------------
    real_awards = award_artists[award_artists["artist_norm"] != UNKNOWN_ARTIST_NORM]
    total_awards = real_awards.groupby("artist_norm")["source_row_number"].nunique()
    artist_tracks = track_artists.merge(tracks[["track_id", "popularity"]], on="track_id")
    track_stats = artist_tracks.groupby("artist_norm").agg(
        track_count=("track_id", "nunique"), avg_popularity=("popularity", "mean"),
        max_popularity=("popularity", "max"))

    norms = sorted(set(spotify_keys) | set(real_awards["artist_norm"]))
    dim_artist = pd.DataFrame({"artist_norm": norms})
    dim_artist["artist_name"] = dim_artist["artist_norm"].map(sp_names).fillna(
        dim_artist["artist_norm"].map(gr_names)).fillna(dim_artist["artist_norm"])
    dim_artist["in_spotify"] = dim_artist["artist_norm"].isin(spotify_keys)
    dim_artist["in_grammy"] = dim_artist["artist_norm"].isin(total_awards.index)
    dim_artist["total_awards"] = dim_artist["artist_norm"].map(total_awards).fillna(0).astype(int)
    dim_artist["track_count"] = dim_artist["artist_norm"].map(track_stats["track_count"]).fillna(0).astype(int)
    dim_artist["avg_popularity"] = dim_artist["artist_norm"].map(track_stats["avg_popularity"]).round(2)
    dim_artist["max_popularity"] = dim_artist["artist_norm"].map(track_stats["max_popularity"])

    # I4: deterministic surrogate keys (sorted business key) -> identical keys on every rerun.
    dim_artist.insert(0, "artist_key", range(1, len(dim_artist) + 1))
    unknown_row = pd.DataFrame([{
        "artist_key": UNKNOWN_ARTIST_KEY, "artist_norm": UNKNOWN_ARTIST_NORM,
        "artist_name": UNKNOWN_ARTIST_NAME, "in_spotify": False, "in_grammy": False,
        "total_awards": int((award_artists["artist_norm"] == UNKNOWN_ARTIST_NORM).sum()),
        "track_count": 0, "avg_popularity": float("nan"), "max_popularity": float("nan"),
    }]).astype({"avg_popularity": "float64", "max_popularity": "float64"})
    dim_artist = pd.concat([unknown_row, dim_artist], ignore_index=True)

    # --- contract checks (cardinality / duplicates) ------------------------------------------
    if dim_artist["artist_norm"].duplicated().any():
        raise ValueError("Integration contract broken: duplicated artist_norm in dim_artist.")
    if award_artists.duplicated(["source_row_number", "artist_norm"]).any():
        raise ValueError("Integration contract broken: an award is linked twice to the same artist.")

    by_method = resolution.drop_duplicates("artist_norm")["match_method"].value_counts()
    matched_names = int(by_method.drop(["unmatched", "non_attributable"], errors="ignore").sum())
    attributable_names = len(grammy_norms) - int(by_method.get("non_attributable", 0))
    awards_with_artist = real_awards["source_row_number"].nunique()
    awards_matched = real_awards.merge(dim_artist[["artist_norm", "in_spotify"]], on="artist_norm")
    awards_matched = awards_matched.loc[awards_matched["in_spotify"], "source_row_number"].nunique()

    unmatched = (
        real_awards.merge(resolution.query("match_method == 'unmatched'")[["resolved_norm"]],
                          left_on="artist_norm", right_on="resolved_norm")
        .groupby("artist_norm")["source_row_number"].nunique().sort_values(ascending=False)
        .rename("awards").reset_index()
    )
    unmatched["artist_name"] = unmatched["artist_norm"].map(gr_names)

    metrics = {
        "grammy_distinct_names": len(grammy_norms),
        "names_by_match_method": {k: int(v) for k, v in by_method.items()},
        "attributable_names": attributable_names,
        "name_match_rate_pct": round(matched_names / max(attributable_names, 1) * 100, 1),
        "composite_names_split": int(by_method.get("composite_split", 0)),
        "awards_with_attributable_artist": int(awards_with_artist),
        "awards_with_spotify_artist": int(awards_matched),
        "award_match_rate_pct": round(awards_matched / max(awards_with_artist, 1) * 100, 1),
        "awards_to_unknown_member": int((award_artists["artist_norm"] == UNKNOWN_ARTIST_NORM).sum()),
        "dim_artist_rows": len(dim_artist),
        "artists_in_both_sources": int((dim_artist["in_spotify"] & dim_artist["in_grammy"]).sum()),
        "grammy_only_artists": int((~dim_artist["in_spotify"] & dim_artist["in_grammy"]).sum()),
        "spotify_only_artists": int((dim_artist["in_spotify"] & ~dim_artist["in_grammy"]).sum()),
    }
    return {"award_artists": award_artists, "dim_artist": dim_artist,
            "unmatched": unmatched, "metrics": metrics}


# ---------------------------------------------------------------------------
# Step 5 - Segments (candidate / consolidated)
# ---------------------------------------------------------------------------
# Popularity measure: MAX popularity of the artist's tracks (best track in the catalog).
# Evidence: the average sinks for large catalogs full of zero-popularity versions
# (e.g. Lady Gaga: max 84, median 0, mean 10, 39 of 67 tracks at 0), which made very popular
# artists look like candidates. If even the best track is below the threshold, the artist
# truly lacks streaming presence.
#
# Thresholds derived from the quartiles of the 2026-10 batch
# (docs/evidence/integration/segment_threshold_analysis.json) and FIXED here on purpose:
# recomputing them on every run would let an artist change segment only because the rest
# of the catalog changed.
SEGMENT_POPULARITY_MEASURE = "max_popularity"
MIN_AWARDS_HIGH_RECOGNITION = 3      # p75 of awards among artists present in both sources
LOW_POPULARITY_BELOW = 25.0          # p25 of max popularity over the whole Spotify catalog
HIGH_POPULARITY_FROM = 41.0          # p50 of max popularity over the whole Spotify catalog
SEGMENT_CANDIDATE, SEGMENT_CONSOLIDATED, SEGMENT_NA = "candidato", "consolidado", "no aplica"


def assign_segments(dim_artist: pd.DataFrame) -> dict:
    """
    Rule SG1 - segment each artist with its MAX track popularity:
      candidato   : >= MIN_AWARDS AND max_popularity <  LOW_POPULARITY_BELOW
      consolidado : >= MIN_AWARDS AND max_popularity >= HIGH_POPULARITY_FROM
      no aplica   : everything else (1-2 awards, middle band, single-source artists,
                    and the "Desconocido" member)
    Only artists present in BOTH sources can be segmented (awards AND popularity are needed).

    Rule SG2 - popularity_zero flags artists whose tracks are ALL at popularity 0
    (max_popularity == 0). They stay candidates (team decision: do not lose candidates),
    but the flag keeps the PR07 limitation visible.
    """
    if LOW_POPULARITY_BELOW is None or HIGH_POPULARITY_FROM is None:
        raise ValueError("Segment thresholds are not set (run scripts/segment_thresholds.py).")
    measure = SEGMENT_POPULARITY_MEASURE
    dim = dim_artist.copy()
    eligible = (dim["in_grammy"] & dim["in_spotify"]
                & (dim["artist_key"] != UNKNOWN_ARTIST_KEY) & dim[measure].notna())
    high_recognition = dim["total_awards"] >= MIN_AWARDS_HIGH_RECOGNITION

    dim["segment"] = SEGMENT_NA
    dim.loc[eligible & high_recognition & (dim[measure] < LOW_POPULARITY_BELOW),
            "segment"] = SEGMENT_CANDIDATE
    dim.loc[eligible & high_recognition & (dim[measure] >= HIGH_POPULARITY_FROM),
            "segment"] = SEGMENT_CONSOLIDATED
    dim["popularity_zero"] = dim["max_popularity"].eq(0).fillna(False).astype(bool)

    candidates = dim[dim["segment"] == SEGMENT_CANDIDATE]
    metrics = {
        "popularity_measure": measure,
        "thresholds": {"min_awards": MIN_AWARDS_HIGH_RECOGNITION,
                       "candidate_popularity_below": LOW_POPULARITY_BELOW,
                       "consolidated_popularity_from": HIGH_POPULARITY_FROM},
        "eligible_artists": int(eligible.sum()),
        "segments": dim["segment"].value_counts().to_dict(),
        "candidates_with_popularity_zero": int(candidates["popularity_zero"].sum()),
        "middle_band_excluded": int((eligible & high_recognition
                                     & dim[measure].between(LOW_POPULARITY_BELOW, HIGH_POPULARITY_FROM,
                                                            inclusive="left")).sum()),
    }
    return {"dim_artist": dim, "metrics": metrics}


# ---------------------------------------------------------------------------
# Step 6 - Assemble the dimensional model and persist the prepared batch
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
EVIDENCE_DIR = Path(os.environ.get("TRANSFORM_EVIDENCE_DIR", "/opt/airflow/docs/evidence/transform"))
MODEL_TABLES = ["dim_artist", "dim_genre", "dim_category", "dim_year",
                "fact_track", "fact_award", "bridge_track_artist", "bridge_track_genre"]


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _sequential_key(df: pd.DataFrame, sort_by: list[str], key: str) -> pd.DataFrame:
    """Rule K1 - deterministic surrogate keys: sort by the business key, number from 1."""
    out = df.sort_values(sort_by, kind="mergesort").reset_index(drop=True)
    out.insert(0, key, range(1, len(out) + 1))
    return out


def _map_fk(values: pd.Series, mapping: pd.Series, fk_name: str) -> pd.Series:
    """Rule K2 - referential integrity: every foreign key must resolve, or the batch fails."""
    mapped = values.map(mapping)
    missing = values[mapped.isna()].unique()
    if len(missing):
        raise ValueError(f"Referential integrity broken for {fk_name}: {list(missing)[:10]}")
    return mapped.astype(int)


def build_model(spotify_raw: pd.DataFrame, grammy_raw: pd.DataFrame) -> dict:
    """Run steps 1-5 and assemble the 8 tables of the star schema with their keys."""
    spotify = prepare_spotify(spotify_raw)
    genres = build_dim_genre(spotify["track_genres"])
    grammy = prepare_grammys(grammy_raw)
    integration = integrate(spotify, grammy)
    segments = assign_segments(integration["dim_artist"])

    dim_artist = segments["dim_artist"][[
        "artist_key", "artist_name", "artist_norm", "in_grammy", "in_spotify", "total_awards",
        "track_count", "avg_popularity", "max_popularity", "segment", "popularity_zero",
    ]]
    dim_genre = _sequential_key(genres["dim_genre"], ["genre"], "genre_key")
    dim_category = _sequential_key(grammy["categories"], ["category_name"], "category_key")
    dim_year = _sequential_key(grammy["years"], ["year"], "year_key")
    fact_track = _sequential_key(spotify["tracks"], ["track_id"], "track_key")

    artist_key = dim_artist.set_index("artist_norm")["artist_key"]
    track_key = fact_track.set_index("track_id")["track_key"]

    bridge_track_artist = pd.DataFrame({
        "track_key": _map_fk(spotify["track_artists"]["track_id"], track_key, "bridge_track_artist.track_key"),
        "artist_key": _map_fk(spotify["track_artists"]["artist_norm"], artist_key, "bridge_track_artist.artist_key"),
    }).drop_duplicates().sort_values(["track_key", "artist_key"]).reset_index(drop=True)

    bridge_track_genre = pd.DataFrame({
        "track_key": _map_fk(spotify["track_genres"]["track_id"], track_key, "bridge_track_genre.track_key"),
        "genre_key": _map_fk(spotify["track_genres"]["track_genre"],
                             dim_genre.set_index("genre")["genre_key"], "bridge_track_genre.genre_key"),
    }).drop_duplicates().sort_values(["track_key", "genre_key"]).reset_index(drop=True)

    awards = integration["award_artists"]
    fact_award = pd.DataFrame({
        "source_row_number": awards["source_row_number"].astype(int),
        "artist_key": _map_fk(awards["artist_norm"], artist_key, "fact_award.artist_key"),
        "category_key": _map_fk(awards["category"], dim_category.set_index("category_name")["category_key"],
                                "fact_award.category_key"),
        "year_key": _map_fk(awards["year"], dim_year.set_index("year")["year_key"], "fact_award.year_key"),
    })
    fact_award["award_count"] = 1
    fact_award = _sequential_key(fact_award, ["source_row_number", "artist_key"], "award_key")

    tables = {
        "dim_artist": dim_artist, "dim_genre": dim_genre, "dim_category": dim_category,
        "dim_year": dim_year, "fact_track": fact_track, "fact_award": fact_award,
        "bridge_track_artist": bridge_track_artist, "bridge_track_genre": bridge_track_genre,
    }
    metrics = {
        "spotify": spotify["metrics"], "genres": genres["metrics"], "grammy": grammy["metrics"],
        "integration": integration["metrics"], "segments": segments["metrics"],
        "table_rows": {name: len(df) for name, df in tables.items()},
        "awards_reconcile_with_source": int(fact_award["source_row_number"].nunique()) == len(grammy_raw),
    }
    return {"tables": tables, "unmatched": integration["unmatched"], "metrics": metrics}


def run_transform(spotify_batch: dict, grammy_batch: dict, run_id: str) -> dict:
    """
    Pipeline entry point (task transform_and_integrate).
    Reads the VALIDATED raw batches, builds the model, writes the prepared tables to
    data/work/<run_id>/prepared/ and the transformation evidence to docs/evidence/transform/<run_id>/.
    Returns only paths and counts (XCom-safe).
    """
    model = build_model(pd.read_csv(spotify_batch["path"]), pd.read_csv(grammy_batch["path"]))

    safe = _safe_run_id(run_id)
    prepared_dir = DATA_DIR / "work" / safe / "prepared"
    evidence_dir = EVIDENCE_DIR / safe
    prepared_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    paths = {}
    for name, df in model["tables"].items():
        path = prepared_dir / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8")
        paths[name] = str(path)

    model["unmatched"].to_csv(evidence_dir / "unmatched_grammy_artists.csv", index=False, encoding="utf-8")
    metrics = {"run_id": run_id, "executed_at_utc": datetime.now(timezone.utc).isoformat(),
               "input_batches": {"spotify": spotify_batch["path"], "grammy": grammy_batch["path"]},
               **model["metrics"]}
    metrics_path = evidence_dir / "transform_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    return {"prepared_dir": str(prepared_dir), "tables": paths,
            "table_rows": model["metrics"]["table_rows"], "metrics_path": str(metrics_path)}