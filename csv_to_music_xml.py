#!/usr/bin/env python3
"""
CSV to Music.app XML Playlist Converter

Converts Spotify CSV playlists (from Exportify) to iTunes XML format
that can be imported into Music.app and used in djay Pro.
"""

import csv
import plistlib
import re
import sys
import os
import glob
import math
import difflib
import argparse
import sqlite3
import time
import concurrent.futures
import hashlib
import threading
import itertools
from pathlib import Path
from unidecode import unidecode
from collections import Counter, defaultdict
from urllib.parse import unquote, urlparse
from rapidfuzz import fuzz
from mutagen import File as MutagenFile
from mutagen.mp4 import MP4

# ----- Default Configuration -----
DEFAULT_LIB_XML = Path("data/MusicLibrary.xml")
DEFAULT_CSV_DIR = Path("data/spotify_csv")
DEFAULT_OUT_DIR = Path("data/music_playlists_xml")
DUR_TOLERANCE_SEC = 3  # ± seconds allowed when matching
USE_ALBUM_IN_MATCH = True  # tighten matching when album is present
REL_TOL = 0.02  # ±2% relative tolerance on duration (adaptive)
ISRC_EXTS = {".m4a", ".mp4", ".mp3", ".flac"}  # formats we'll probe for ISRC


# ----- ISRC Cache (SQLite) -----
def open_isrc_cache(path: Path):
    """Open SQLite cache for ISRC lookups."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # allow cross-thread usage if it ever sneaks in, but we'll still write on the main thread
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS isrc_cache (
            pid TEXT PRIMARY KEY,
            path TEXT,
            mtime REAL,
            size INTEGER,
            isrc TEXT,
            status INTEGER,     -- 1=found, 0=none, -1=error
            updated REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_isrc ON isrc_cache(isrc)")
    conn.commit()
    return conn


def load_isrc_cache(conn):
    """Load cache into memory for fast lookups."""
    by_pid = {}
    by_isrc = {}
    for pid, path, mtime, size, isrc, status, updated in conn.execute(
        "SELECT pid, path, mtime, size, isrc, status, updated FROM isrc_cache"
    ):
        rec = {
            "path": path,
            "mtime": mtime,
            "size": size,
            "isrc": isrc,
            "status": status,
            "updated": updated,
        }
        by_pid[pid] = rec
        if status == 1 and isrc:
            by_isrc[isrc.upper()] = pid
    return by_pid, by_isrc


def stat_file(p):
    """Get file modification time and size."""
    try:
        st = os.stat(p)
        return st.st_mtime, st.st_size
    except Exception:
        return None, None


def cache_upsert(conn, pid, path, mtime, size, isrc, status):
    """Update or insert ISRC cache entry."""
    conn.execute(
        "INSERT INTO isrc_cache (pid, path, mtime, size, isrc, status, updated) VALUES (?,?,?,?,?,?,?) "
        "ON CONFLICT(pid) DO UPDATE SET path=excluded.path, mtime=excluded.mtime, size=excluded.size, "
        "isrc=excluded.isrc, status=excluded.status, updated=excluded.updated",
        (pid, path, mtime, size, isrc, status, time.time()),
    )


def is_supported_audio(path):
    """Check if file format supports ISRC tags."""
    ext = Path(path).suffix.lower()
    return ext in ISRC_EXTS


# ----- Helper Functions -----
def norm(s):
    """Normalize string for matching: lowercase, remove accents, collapse whitespace."""
    if s is None:
        return ""
    s = unidecode(s).lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def key_variants(artist, title, album, ms):
    """Generate matching keys to increase hit rate."""
    a = norm(artist)
    t = norm(title)
    al = norm(album)
    secs = None
    try:
        secs = round(float(ms) / 1000.0) if ms else None
    except (ValueError, TypeError):
        pass

    keys = []
    base = f"{a}|{t}"

    if USE_ALBUM_IN_MATCH and al:
        keys.append((f"{base}|{al}", secs))
    keys.append((base, secs))

    return keys


def dur_close(a, b, tol=DUR_TOLERANCE_SEC, rel=REL_TOL):
    """Check if two durations are within tolerance."""
    if a is None or b is None:
        return True  # duration unknown -> allow
    a = int(a)
    b = int(b)
    adaptive = max(tol, math.ceil(rel * max(a, b)))
    return abs(a - b) <= adaptive


_SIMPLIFY_PATTERNS = [
    (
        re.compile(
            r"\s*-\s*(remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended|deluxe).*$",
            re.I,
        ),
        "",
    ),
    (
        re.compile(
            r"\s*\((?:feat\.?|featuring|with|remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended).*?\)\s*$",
            re.I,
        ),
        "",
    ),
    (re.compile(r"\s*\[.*?\]\s*$"), ""),
]


def simplify_title(s: str) -> str:
    """Strip common qualifiers: remastered, feat, radio edit, etc."""
    s = s or ""
    for pat, repl in _SIMPLIFY_PATTERNS:
        s = pat.sub(repl, s)
    return s.strip()


def location_to_path(location_url):
    """Convert iTunes file:// URL to local file path."""
    if not location_url:
        return None
    try:
        # Parse the URL and unquote special characters
        parsed = urlparse(location_url)
        if parsed.scheme == "file":
            # Remove 'localhost' if present, handle file:// URLs
            path = parsed.path
            if parsed.netloc == "localhost":
                path = parsed.path
            # Unquote URL encoding (e.g., %20 -> space)
            path = unquote(path)
            return path
    except Exception:
        pass
    return None


def canon_name(name: str) -> str:
    """Canonicalize playlist name for stable ID generation."""
    s = unidecode(name or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def derive_playlist_ids(name: str, source_id: str | None = None, salt: str = "itxml:v1"):
    """
    Generate deterministic playlist IDs from canonical identity.
    
    Returns (playlist_id_int, playlist_persistent_id_hex16)
    - source_id: if you have a Spotify playlist URI/ID, pass it; else None.
    """
    ident = source_id or canon_name(name)
    payload = (salt + "\x1f" + ident).encode("utf-8")
    h = hashlib.sha1(payload).digest()
    pid_hex = h.hex().upper()[:16]                 # 16 hex chars
    int_id  = int.from_bytes(h[:4], "big") & 0x7FFFFFFF
    if int_id == 0:
        int_id = 1
    return int_id, pid_hex


def extract_isrc_from_file(filepath):
    """Extract ISRC from audio file using mutagen."""
    if not filepath or not is_supported_audio(filepath):
        return None

    try:
        audio = MutagenFile(filepath)
        if audio is None:
            return None

        # MP4/M4A files (iTunes)
        if isinstance(audio, MP4):
            # Try standard ISRC atom
            if "----:com.apple.iTunes:ISRC" in audio:
                isrc_data = audio["----:com.apple.iTunes:ISRC"][0]
                if hasattr(isrc_data, "decode"):
                    return isrc_data.decode("utf-8").strip()
                return str(isrc_data).strip()

        # ID3 tags (MP3, etc)
        elif hasattr(audio, "tags"):
            tags = audio.tags
            if tags:
                # Standard ISRC frame
                if "TSRC" in tags:
                    return str(tags["TSRC"][0]).strip()
                # Sometimes stored in TXXX frames
                for key, value in tags.items():
                    if key.startswith("TXXX:") and "ISRC" in key.upper():
                        return str(value[0]).strip()

        # FLAC/Vorbis comments
        elif hasattr(audio, "get"):
            if "ISRC" in audio:
                return str(audio["ISRC"][0]).strip()

    except Exception:
        # Silently fail - ISRCs are optional
        pass

    return None


# ----- Load Music Library XML and Build Index -----
def build_music_index(lib_xml, cache_conn=None):
    """Parse Music.app Library XML and build searchable index."""
    if not lib_xml.exists():
        print(f"ERROR: {lib_xml} not found!")
        print("\nTo export your Music Library:")
        print("1. Open Music.app")
        print("2. Go to File → Library → Export Library...")
        print("3. Save as 'MusicLibrary.xml' in this directory")
        sys.exit(1)

    print(f"Loading Music library from {lib_xml}...")

    with open(lib_xml, "rb") as f:
        lib = plistlib.load(f)

    tracks = lib.get("Tracks", {})
    index = {}
    local_count = 0

    # Auxiliary structures for diagnostics
    by_title = defaultdict(list)  # norm(title) -> [rec...]
    by_artist = defaultdict(list)  # norm(artist) -> [rec...]
    base_key_index = defaultdict(list)  # norm(artist)|norm(title) -> [rec...]
    by_album = defaultdict(list)  # norm(album) -> [rec...] with disc/track numbers
    by_isrc = {}  # ISRC -> pid (exact match, from CACHE ONLY at build time)
    by_secs = defaultdict(list)  # int seconds -> [rec...]
    by_tid = {}  # track_id -> rec for writing Tracks dict

    # Build index: (artist|title[|album]) -> list of {pid, track_id, secs}
    for tid, td in tracks.items():
        # Only index items that have a file location (local files)
        if not td.get("Location"):
            continue

        local_count += 1
        artist = td.get("Artist") or td.get("Album Artist") or ""
        title = td.get("Name") or ""
        album = td.get("Album") or ""
        disc_no = td.get("Disc Number") or 1
        track_no = td.get("Track Number") or None
        location = td.get("Location")
        secs = None

        if "Total Time" in td:
            try:
                secs = round(td["Total Time"] / 1000.0)
            except (ValueError, TypeError, KeyError):
                pass

        # ISRC at index-build time comes ONLY from cache; file reads happen lazily later
        isrc = None

        variants = key_variants(artist, title, album, td.get("Total Time"))
        na, nt, nal = norm(artist), norm(title), norm(album)

        rec = {
            "track_id": int(tid),
            "pid": td.get("Persistent ID"),
            "secs": secs,
            "artist": artist,
            "title": title,
            "album": album,
            "disc_no": disc_no,
            "track_no": track_no,
            "location": location,
            "isrc": isrc,
        }

        # Add to diagnostic structures
        by_title[nt].append(rec)
        by_artist[na].append(rec)
        base_key_index[f"{na}|{nt}"].append(rec)
        by_tid[int(tid)] = rec

        # Add to album index
        if nal:
            by_album[nal].append(rec)

        # Add to duration index
        if secs is not None:
            by_secs[int(secs)].append(rec)

        for k, _csv_secs in variants:
            bucket = index.setdefault(k, [])
            bucket.append(rec)

    print(f"Indexed {local_count} local tracks from Music library")

    diag = {
        "by_title": by_title,
        "by_artist": by_artist,
        "by_album": by_album,
        "by_isrc": by_isrc,
        "by_secs": by_secs,
        "base_key_index": base_key_index,
        "artists": set(by_artist.keys()),
        "titles": set(by_title.keys()),
        "by_tid": by_tid,
    }

    return index, diag


# ----- Playlist XML Writer -----
def write_playlist_xml(playlist_name, track_ids, out_dir, by_tid):
    """Write iTunes/Music XML with proper Tracks dictionary and playlist entries."""
    # Build Tracks dict: keys must be strings of Track IDs
    tracks_dict = {}
    for tid in track_ids:
        rec = by_tid.get(int(tid))
        if not rec: 
            continue
        # Use the exact Location string from the library export (already file:// URL)
        track_entry = {
            "Track ID": int(tid),
            "Name": rec.get("title") or "",
            "Artist": rec.get("artist") or "",
            "Album": rec.get("album") or "",
            "Track Type": "File",
        }
        if rec.get("secs") is not None:
            track_entry["Total Time"] = int(rec["secs"]) * 1000
        if rec.get("pid"):
            track_entry["Persistent ID"] = rec["pid"]
        if rec.get("location"):
            track_entry["Location"] = rec["location"]  # DO NOT unquote; keep as exported
        tracks_dict[str(tid)] = track_entry

    # Generate deterministic playlist IDs
    plist_id, plist_pid = derive_playlist_ids(playlist_name)

    plist = {
        "Major Version": 1,
        "Minor Version": 1,
        "Application Version": "13.0",
        "Features": 5,
        "Show Content Ratings": True,
        "Tracks": tracks_dict,
        "Playlists": [
            {
                "Name": playlist_name,
                "Playlist ID": plist_id,
                "Playlist Persistent ID": plist_pid,
                "All Items": True,
                "Playlist Items": [{"Track ID": int(tid)} for tid in track_ids],
            }
        ],
    }

    out = out_dir / f"{playlist_name}.xml"
    with open(out, "wb") as fh:
        plistlib.dump(plist, fh, sort_keys=False)
    return out


# ----- Track Matching -----
def best_match(row, index, diag, args):
    """Find best matching track in Music library for a CSV row."""
    # Read Exportify's exact column names
    title = row.get("Track Name", "") or row.get("track_name", "")
    title_simpl = simplify_title(title)
    artists_field = row.get("Artist Name(s)", "") or row.get("artist_name(s)", "")
    album = row.get("Album Name", "") or row.get("album_name", "")
    album_norm = norm(album)
    isrc = (row.get("ISRC") or "").strip()
    isrc_u = isrc.upper() if isrc else ""

    # Handle multiple artists - take first as primary, but also try full string
    primary_artist = artists_field.split(",")[0].strip() if artists_field else ""

    # Extract duration from Exportify's column name
    ms = None
    if row.get("Track Duration (ms)"):
        try:
            ms = float(row["Track Duration (ms)"])
        except (ValueError, TypeError, KeyError):
            pass
    secs = round(ms / 1000.0) if ms else None

    # Parse disc and track numbers from CSV
    def parse_int(x):
        try:
            return int(str(x).strip())
        except (ValueError, TypeError, AttributeError):
            return None

    disc_csv = parse_int(row.get("Disc Number"))
    track_csv = parse_int(row.get("Track Number"))

    # 0) ISRC exact match via CACHE (populated in main/process)
    if isrc_u and isrc_u in diag["by_isrc"]:
        pid = diag["by_isrc"][isrc_u]
        return diag["by_pid_map"].get(pid)  # map pid -> rec

    # 1) Strong album+disc/track match
    if album_norm and diag["by_album"].get(album_norm):
        candidates = diag["by_album"][album_norm]
        # Prefer exact disc/track, else fallback by closest duration
        exact = [
            c
            for c in candidates
            if (track_csv and c["track_no"] == track_csv)
            and ((disc_csv or 1) == (c["disc_no"] or 1))
        ]
        if exact:
            # duration guard if available
            if secs is None or any(dur_close(c["secs"], secs) for c in exact):
                return sorted(
                    exact,
                    key=lambda c: 0 if secs is None else abs((c["secs"] or 0) - secs),
                )[0]
        # fallback: same album, same (simplified) title within duration band
        simp_nt = norm(title_simpl)
        near = [c for c in candidates if dur_close(c["secs"], secs)]
        near_title = [c for c in near if norm(simplify_title(c["title"])) == simp_nt]
        if near_title:
            return near_title[0]

    # 2) Try multiple key variants with simplified title
    keys_to_try = []

    # Primary artist keys with both original and simplified titles
    if primary_artist:
        # Try simplified title first
        if title_simpl != title:
            prim_keys_simpl = key_variants(primary_artist, title_simpl, album, ms)
            keys_to_try.extend(prim_keys_simpl)
        # Then original title
        prim_keys = key_variants(primary_artist, title, album, ms)
        keys_to_try.extend(prim_keys)

    # Full artist string keys (if different from primary)
    if artists_field and artists_field != primary_artist:
        if title_simpl != title:
            full_keys_simpl = key_variants(artists_field, title_simpl, album, ms)
            keys_to_try.extend(full_keys_simpl)
        full_keys = key_variants(artists_field, title, album, ms)
        keys_to_try.extend(full_keys)

    # Try each key variant
    for k, csv_secs in keys_to_try:
        if k in index:
            # Choose candidate with closest duration
            cands = sorted(
                index[k],
                key=lambda c: 0
                if dur_close(c["secs"], csv_secs)
                else abs((c["secs"] or 0) - (csv_secs or 0)),
            )

            for c in cands:
                if dur_close(c["secs"], csv_secs):
                    return c

            # Fallback to first candidate if no duration match
            if cands:
                return cands[0]

    # 3) Optional guarded fuzzy fallback (only when we have album hit or duration guard)
    # search in album bucket if available, else scan all keys for artist match
    pool = diag["by_album"].get(album_norm, [])
    if not pool and primary_artist:
        # cheap pool: all entries reachable via primary_artist+any title key
        pa = norm(primary_artist)
        pool = []
        # gather all tracks by scanning index buckets that start with pa|
        for k, lst in index.items():
            if k.startswith(f"{pa}|"):
                pool.extend(lst)
    if pool:
        best = None
        best_score = 0
        nt = norm(title_simpl or title)
        for c in pool:
            if (
                secs is not None
                and c["secs"] is not None
                and not dur_close(c["secs"], secs)
            ):
                continue
            score = fuzz.token_set_ratio(nt, norm(c["title"]))
            if score > best_score:
                best = c
                best_score = score
        if best and best_score >= 95:
            return best

    # 4) Lazy ISRC confirmation on a *small* candidate pool (budgeted)
    if isrc_u and args.isrc:
        cand = []
        # Prefer album pool
        if album_norm and diag["by_album"].get(album_norm):
            cand.extend(diag["by_album"][album_norm])
        # Add duration band pool
        if secs is not None and diag.get("by_secs"):
            tol = max(DUR_TOLERANCE_SEC, math.ceil(REL_TOL * secs))
            for s in range(max(0, int(secs) - tol), int(secs) + tol + 1):
                cand.extend(diag["by_secs"].get(s, []))
        # Add primary-artist pool
        if primary_artist:
            pa = norm(primary_artist)
            for k, lst in index.items():
                if k.startswith(f"{pa}|"):
                    cand.extend(lst)
        # Dedup by pid and cap
        seen = set()
        uniq = []
        for c in cand:
            pid = c.get("pid")
            if pid and pid not in seen:
                seen.add(pid)
                uniq.append(c)
        uniq = uniq[: args.isrc_probe_cap]
        # Probe cache and read on-demand within budget
        rec = lazy_isrc_confirm(uniq, isrc_u, diag, args)
        if rec:
            return rec

    return None


# ----- Diagnostic Functions -----
def summarize_candidate(c):
    """Format a candidate track for display."""
    return f"[id {c['track_id']}] {c['artist']} — {c['title']} · {c['album']} · {c['secs']}s"


def simplify_title_normed(s: str) -> str:
    """Strip common qualifiers: remastered, feat, radio edit, etc."""
    s = re.sub(
        r"\s*-\s*(remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended|deluxe).*$",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(
        r"\s*\((?:feat\.?|featuring|with|remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended).*?\)\s*$",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r"\s*\[.*?\]\s*$", "", s)
    return s.strip()


def diagnose_row(row, index, diag, playlist_name, emitted_count, suggestions_per_row):
    """Print why a row failed to match and suggest nearest candidates."""
    title = row.get("Track Name", "") or row.get("track_name", "")
    artists_field = row.get("Artist Name(s)", "") or row.get("artist_name(s)", "")
    album = row.get("Album Name", "") or row.get("album_name", "")
    primary_artist = artists_field.split(",")[0].strip() if artists_field else ""

    ms = None
    if row.get("Track Duration (ms)"):
        try:
            ms = float(row["Track Duration (ms)"])
        except (ValueError, TypeError, KeyError):
            pass

    # Get all keys that would be tried
    cand_keys = []
    if primary_artist:
        cand_keys += key_variants(primary_artist, title, album, ms)
    if artists_field and artists_field != primary_artist:
        cand_keys += key_variants(artists_field, title, album, ms)

    na, nt, nal = norm(primary_artist), norm(title), norm(album)
    secs = round(float(ms) / 1000.0) if ms else None

    print(f"\n— DEBUG({playlist_name}) #{emitted_count + 1}")
    print(f"  CSV: {artists_field} — {title} · {album} · {secs}s")
    print(f"  norm: {na} — {nt} · {nal}")
    print("  keys tried (order):")

    for k, _ in cand_keys:
        present = k in index
        count = len(index.get(k, []))
        print(
            f"    • {k} -> {'HIT' if present else 'MISS'} ({count} candidate{'s' if count != 1 else ''})"
        )

        if present:
            # Show top 3 candidates nearest in duration
            cands = index[k]

            def score(c):
                if secs is None or c["secs"] is None:
                    return 9999
                return abs(c["secs"] - secs)

            for c in sorted(cands, key=score)[: min(3, len(cands))]:
                d = None if secs is None or c["secs"] is None else c["secs"] - secs
                print(f"        - {summarize_candidate(c)}  Δt={d}")

    # If all keys missed, provide suggestions
    if all(k not in index for k, _ in cand_keys):
        title_hits = diag["by_title"].get(nt, [])
        if title_hits:
            print("  Exact title exists in library (different artist/album):")
            for c in title_hits[:suggestions_per_row]:
                print(f"    · {summarize_candidate(c)}")

        artist_bucket = diag["by_artist"].get(na, [])
        if artist_bucket:
            ratios = [
                (difflib.SequenceMatcher(None, nt, norm(c["title"])).ratio(), c)
                for c in artist_bucket
            ]
            top = [
                c
                for r, c in sorted(ratios, key=lambda x: x[0], reverse=True)
                if r >= 0.70
            ][:suggestions_per_row]
            if top:
                print("  Close titles for same artist (≥0.70):")
                for c in top:
                    print(f"    · {summarize_candidate(c)}")

        title_bucket = diag["by_title"].get(nt, [])
        if title_bucket:
            ratios = [
                (difflib.SequenceMatcher(None, na, norm(c["artist"])).ratio(), c)
                for c in title_bucket
            ]
            top = [
                c
                for r, c in sorted(ratios, key=lambda x: x[0], reverse=True)
                if r >= 0.70
            ][:suggestions_per_row]
            if top:
                print("  Close artists for same title (≥0.70):")
                for c in top:
                    print(f"    · {summarize_candidate(c)}")

        # What-if: strip qualifiers from title
        alt_nt = simplify_title_normed(nt)
        if alt_nt != nt:
            base_key = f"{na}|{alt_nt}"
            bucket = diag["base_key_index"].get(base_key, [])
            if bucket:
                print("  Would match if title qualifiers removed:")
                for c in bucket[:suggestions_per_row]:
                    print(f"    · {summarize_candidate(c)}")


# ----- Main Processing -----
def process_playlists(index, diag, args):
    """Process all CSV playlists and convert to XML."""
    if not args.csv_dir.exists():
        print(f"\nERROR: CSV directory '{args.csv_dir}' not found!")
        print("\nTo export your Spotify playlists:")
        print("1. Go to https://exportify.net")
        print("2. Log in with Spotify")
        print("3. Click 'Export All'")
        print("4. Extract the ZIP to a folder called 'spotify_csv' in this directory")
        sys.exit(1)

    csv_files = list(glob.glob(str(args.csv_dir / "*.csv")))

    if not csv_files:
        print(f"\nNo CSV files found in {args.csv_dir}/")
        print("Make sure you've extracted your Exportify export to this directory.")
        sys.exit(1)

    print(f"\nFound {len(csv_files)} CSV playlists to process")

    args.output.mkdir(parents=True, exist_ok=True)

    unmatched_report = []
    unmatched_by_artist = Counter()
    unmatched_by_title = Counter()
    processed_count = 0
    total_tracks = 0

    for csv_path in csv_files:
        pl_name = Path(csv_path).stem
        track_ids = []
        unmatched_in_playlist = []
        debug_emitted = 0

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = best_match(row, index, diag, args)
                if m:
                    track_ids.append(m["track_id"])
                else:
                    # Use the exact column names from Exportify
                    artist = row.get("Artist Name(s)", "") or row.get(
                        "artist_name(s)", ""
                    )
                    title = row.get("Track Name", "") or row.get("track_name", "")
                    unmatched_in_playlist.append((artist, title))
                    unmatched_report.append((pl_name, artist, title))
                    unmatched_by_artist[norm(artist)] += 1
                    unmatched_by_title[norm(title)] += 1

                    # Debug diagnostics if enabled
                    if args.debug and diag and debug_emitted < args.limit:
                        diagnose_row(
                            row, index, diag, pl_name, debug_emitted, args.suggestions
                        )
                        debug_emitted += 1

        if track_ids:
            write_playlist_xml(pl_name, track_ids, args.output, diag["by_tid"])
            processed_count += 1
            total_tracks += len(track_ids)
            print(f"✓ {pl_name}: {len(track_ids)} tracks matched", end="")
            if unmatched_in_playlist:
                print(f" ({len(unmatched_in_playlist)} unmatched)")
            else:
                print()
        else:
            print(f"✗ {pl_name}: No matches found")

    # Save unmatched report
    if unmatched_report:
        rpt = args.output / "_unmatched.tsv"
        with open(rpt, "w", encoding="utf-8") as fh:
            fh.write("Playlist\tArtist\tTrack\n")
            for pl, a, t in unmatched_report:
                fh.write(f"{pl}\t{a}\t{t}\n")
        print(f"\n{len(unmatched_report)} unmatched tracks saved to {rpt}")

    print("\nConversion complete!")
    print(f"• Processed {processed_count}/{len(csv_files)} playlists")
    print(f"• Total tracks: {total_tracks}")
    print(f"• Output directory: {args.output}/")

    if unmatched_report:
        print("\nTop unmatched artists:")
        for a, cnt in unmatched_by_artist.most_common(10):
            print(f"  {a}  ×{cnt}")
        print("\nTop unmatched titles:")
        for t, cnt in unmatched_by_title.most_common(10):
            print(f"  {t}  ×{cnt}")

    return processed_count


def lazy_isrc_confirm(candidates, target_isrc_u, diag, args):
    """
    Try to confirm a match by ISRC for a small candidate set.
    Uses cache first; reads tags if needed but respects a global budget.
    """
    # Fast path: cache check
    for c in candidates:
        pid = c.get("pid")
        if not pid:
            continue
        ce = diag["isrc_cache_by_pid"].get(pid)
        if ce and ce.get("status") == 1 and ce.get("isrc"):
            if ce["isrc"].upper() == target_isrc_u:
                return c
        if ce and ce.get("status") == 0:
            continue  # negative cached

    # Read path(s) if budget allows
    lock = diag["isrc_budget_lock"]
    with lock:
        if diag["isrc_reads_left"] <= 0:
            return None
        budget = min(diag["isrc_reads_left"], len(candidates), args.workers)
        diag["isrc_reads_left"] -= budget

    to_probe = []
    for c in candidates:
        pid = c.get("pid")
        loc = c.get("location")
        if not pid or not loc:
            continue
        if pid in diag["isrc_cache_by_pid"] and diag["isrc_cache_by_pid"][pid].get(
            "status"
        ) in (0, 1):
            continue
        p = location_to_path(loc)
        if not p or not is_supported_audio(p):
            continue
        mtime, size = stat_file(p)
        to_probe.append((c, pid, p, mtime, size))
        if len(to_probe) >= budget:
            break

    def probe(entry):
        # worker: read tags only
        c, pid, p, mtime, size = entry
        isrc = None
        status = -1
        try:
            isrc = extract_isrc_from_file(p)
            status = 1 if isrc else 0
        except Exception:
            status = -1
        return (c, pid, p, mtime, size, isrc, status)

    if to_probe:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for c, pid, p, mtime, size, isrc_found, status in ex.map(probe, to_probe):
                # main thread: persist and update in-memory
                cache_upsert(diag["isrc_cache_conn"], pid, p, mtime or 0.0, size or 0, isrc_found, status)
                diag["isrc_cache_by_pid"][pid] = {
                    "path": p, "mtime": mtime, "size": size,
                    "isrc": isrc_found, "status": status, "updated": time.time(),
                }
                if status == 1 and isrc_found:
                    diag["by_isrc"][isrc_found.upper()] = pid
                if isrc_found and isrc_found.upper() == target_isrc_u:
                    diag["isrc_cache_conn"].commit()
                    return c
        diag["isrc_cache_conn"].commit()
    return None


def gather_csv_albums_and_isrcs(csv_dir):
    """Gather album names and check for ISRC presence in CSV files."""
    albums = set()
    has_isrc_rows = False
    for csv_path in glob.glob(str(csv_dir / "*.csv")):
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in itertools.islice(reader, 0, 2000):  # limit scan
                    a = row.get("Album Name") or row.get("album_name") or ""
                    if a:
                        albums.add(norm(a))
                    isrc = (row.get("ISRC") or "").strip()
                    if isrc and len(isrc.strip()) >= 10:
                        has_isrc_rows = True
        except Exception:
            continue
    return albums, has_isrc_rows


# ----- Main Entry Point -----
def check_csvs_for_isrcs(csv_dir):
    """Quick check if any CSV files contain ISRCs."""
    if not csv_dir.exists():
        return False

    csv_files = list(glob.glob(str(csv_dir / "*.csv")))[:5]  # Check first 5 files
    for csv_path in csv_files:
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i > 10:  # Check first 10 rows
                        break
                    isrc = (row.get("ISRC") or "").strip()
                    if isrc and len(isrc) == 12:  # ISRCs are 12 chars
                        return True
        except Exception:
            pass
    return False


def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Convert Spotify CSV playlists to Music.app XML format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Basic conversion
  %(prog)s --debug                  # Show diagnostics for unmatched tracks
  %(prog)s --debug --limit 10       # Show more debug info per playlist
  %(prog)s --isrc                   # Enable ISRC matching (slow but accurate)
  %(prog)s --library ~/Music.xml    # Use custom library location
        """,
    )

    # File locations
    parser.add_argument(
        "--library",
        type=Path,
        default=DEFAULT_LIB_XML,
        help="Music library XML file (default: %(default)s)",
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=DEFAULT_CSV_DIR,
        help="Directory containing Spotify CSV files (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Output directory for playlist XML files (default: %(default)s)",
    )

    # Matching options
    parser.add_argument(
        "--isrc",
        action="store_true",
        help="Enable ISRC-assisted matching with lazy, cached lookups (fast)",
    )
    parser.add_argument(
        "--isrc-all",
        action="store_true",
        help="Eagerly read ISRC tags for the entire library (slow; not recommended)",
    )
    parser.add_argument(
        "--isrc-prefetch",
        choices=["none", "albums"],
        default="albums",
        help="Prefetch ISRCs for CSV-referenced albums before matching (default: albums)",
    )
    parser.add_argument(
        "--isrc-cache",
        type=Path,
        default=Path("data/isrc_cache.sqlite"),
        help="Path to persistent ISRC cache (default: %(default)s)",
    )
    parser.add_argument(
        "--isrc-probe-cap",
        type=int,
        default=64,
        help="Max candidate files to probe per row for ISRC (default: %(default)s)",
    )
    parser.add_argument(
        "--max-tag-reads",
        type=int,
        default=500,
        help="Global cap on file tag reads this run (default: %(default)s)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel workers for tag reads (default: %(default)s)",
    )

    # Debug options
    parser.add_argument(
        "--debug", action="store_true", help="Show diagnostic info for unmatched tracks"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        metavar="N",
        help="Number of unmatched tracks to diagnose per playlist (default: %(default)s)",
    )
    parser.add_argument(
        "--suggestions",
        type=int,
        default=3,
        metavar="N",
        help="Number of suggestions to show per unmatched track (default: %(default)s)",
    )

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    print("CSV to Music.app XML Playlist Converter")
    print("=" * 40)

    # ISRC cache
    cache_conn = open_isrc_cache(args.isrc_cache)
    isrc_cache_by_pid, cache_by_isrc = load_isrc_cache(cache_conn)
    if args.isrc and cache_by_isrc:
        print(f"ISRC cache warm: {len(cache_by_isrc)} mappings loaded.")

    # Build index from Music Library (no eager reads; we'll use cache)
    index, diag = build_music_index(args.library, cache_conn)

    # Map pid -> rec for fast lookup from cache hits
    by_pid_map = {}
    for buckets in diag["by_album"].values():
        for r in buckets:
            if r.get("pid"):
                by_pid_map[r["pid"]] = r
    diag["by_pid_map"] = by_pid_map

    # Seed by_isrc from cache against current pid map
    for isrc_u, pid in list(cache_by_isrc.items()):
        if pid in by_pid_map:
            diag["by_isrc"][isrc_u] = pid

    # Global budget for lazy reads
    diag["isrc_reads_left"] = args.max_tag_reads
    diag["isrc_budget_lock"] = threading.Lock()
    diag["isrc_cache_conn"] = cache_conn
    diag["isrc_cache_by_pid"] = isrc_cache_by_pid

    # Optional prefetch limited to CSV albums (keeps reads tractable)
    if args.isrc and args.isrc_prefetch == "albums":
        csv_albums, has_isrc_rows = gather_csv_albums_and_isrcs(args.csv_dir)
        if csv_albums:
            to_prefetch = []
            for a in csv_albums:
                for r in diag["by_album"].get(a, []):
                    pid = r.get("pid")
                    loc = r.get("location")
                    if not pid or not loc:
                        continue
                    if pid in isrc_cache_by_pid and isrc_cache_by_pid[pid].get(
                        "status"
                    ) in (0, 1):
                        continue
                    p = location_to_path(loc)
                    if not p or not is_supported_audio(p):
                        continue
                    mtime, size = stat_file(p)
                    to_prefetch.append((r, pid, p, mtime, size))
            # bound prefetch by budget
            prefetch_n = max(0, min(args.max_tag_reads // 2, len(to_prefetch)))
            if prefetch_n:
                print(f"Prefetching ISRCs for {prefetch_n} CSV-album tracks...")

                def _probe(e):
                    # worker thread: read tags only, no DB writes
                    r, pid, p, mtime, size = e
                    isrc = None
                    status = -1
                    try:
                        isrc = extract_isrc_from_file(p)
                        status = 1 if isrc else 0
                    except Exception:
                        status = -1
                    return (pid, p, mtime, size, isrc, status)

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=args.workers
                ) as ex:
                    for pid, p, mtime, size, isrc, status in ex.map(_probe, to_prefetch[:prefetch_n]):
                        # main thread: update DB and in-memory caches
                        cache_upsert(cache_conn, pid, p, mtime or 0.0, size or 0, isrc, status)
                        diag["isrc_cache_by_pid"][pid] = {
                            "path": p, "mtime": mtime, "size": size,
                            "isrc": isrc, "status": status, "updated": time.time(),
                        }
                        if status == 1 and isrc:
                            diag["by_isrc"][isrc.upper()] = pid
                cache_conn.commit()
                diag["isrc_reads_left"] = max(0, args.max_tag_reads - prefetch_n)

    # Process all CSV playlists
    processed = process_playlists(index, diag, args)

    if processed > 0:
        print("\nNext steps:")
        print("1. Open Music.app")
        print("2. Go to File → Library → Import Playlist...")
        print(f"3. Select all XML files in '{args.output}/' (Cmd+A)")
        print("4. Click 'Open'")
        print("5. Your playlists will appear in Music.app and djay Pro!")
        print("\nNote: Only tracks already in your Music library will be added.")


if __name__ == "__main__":
    main()
