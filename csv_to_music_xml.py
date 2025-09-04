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
from pathlib import Path
from unidecode import unidecode
from collections import Counter, defaultdict

# ----- Configuration -----
LIB_XML = Path("data/MusicLibrary.xml")             # exported from Music.app
CSV_DIR = Path("data/spotify_csv")                  # folder of Exportify CSVs  
OUT_DIR = Path("data/music_playlists_xml")          # where we write playlist XMLs
DUR_TOLERANCE_SEC = 3                               # ± seconds allowed when matching
USE_ALBUM_IN_MATCH = True                           # tighten matching when album is present
DEBUG = int(os.environ.get("DEBUG", "0"))           # 0=off, 1=on
DEBUG_LIMIT_PER_PLAYLIST = int(os.environ.get("DEBUG_LIMIT", "5"))
SUGGESTIONS_PER_ROW = int(os.environ.get("SUGGESTIONS", "3"))

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
        secs = round(float(ms)/1000.0) if ms else None
    except: 
        pass
    
    keys = []
    base = f"{a}|{t}"
    
    if USE_ALBUM_IN_MATCH and al:
        keys.append((f"{base}|{al}", secs))
    keys.append((base, secs))
    
    return keys

def dur_close(a, b, tol=DUR_TOLERANCE_SEC):
    """Check if two durations are within tolerance."""
    if a is None or b is None: 
        return True  # duration unknown -> allow
    return abs(int(a) - int(b)) <= tol

# ----- Load Music Library XML and Build Index -----
def build_music_index():
    """Parse Music.app Library XML and build searchable index."""
    if not LIB_XML.exists():
        print(f"ERROR: {LIB_XML} not found!")
        print("\nTo export your Music Library:")
        print("1. Open Music.app")
        print("2. Go to File → Library → Export Library...")
        print("3. Save as 'MusicLibrary.xml' in this directory")
        sys.exit(1)
    
    print(f"Loading Music library from {LIB_XML}...")
    
    with open(LIB_XML, 'rb') as f:
        lib = plistlib.load(f)
    
    tracks = lib.get("Tracks", {})
    index = {}
    local_count = 0
    
    # Auxiliary structures for diagnostics
    by_title = defaultdict(list)        # norm(title) -> [rec...]
    by_artist = defaultdict(list)       # norm(artist) -> [rec...]
    base_key_index = defaultdict(list)  # norm(artist)|norm(title) -> [rec...]
    
    # Build index: (artist|title[|album]) -> list of {pid, track_id, secs}
    for tid, td in tracks.items():
        # Only index items that have a file location (local files)
        if not td.get("Location"): 
            continue
            
        local_count += 1
        artist = td.get("Artist") or td.get("Album Artist") or ""
        title = td.get("Name") or ""
        album = td.get("Album") or ""
        secs = None
        
        if "Total Time" in td:
            try: 
                secs = round(td["Total Time"]/1000.0)
            except: 
                pass
                
        variants = key_variants(artist, title, album, td.get("Total Time"))
        na, nt, nal = norm(artist), norm(title), norm(album)
        
        rec = {
            "track_id": int(tid),
            "pid": td.get("Persistent ID"),
            "secs": secs,
            "artist": artist,
            "title": title,
            "album": album,
        }
        
        # Add to diagnostic structures
        by_title[nt].append(rec)
        by_artist[na].append(rec)
        base_key_index[f"{na}|{nt}"].append(rec)
        
        for (k, _csv_secs) in variants:
            bucket = index.setdefault(k, [])
            bucket.append(rec)
    
    print(f"Indexed {local_count} local tracks from Music library")
    
    diag = {
        "by_title": by_title,
        "by_artist": by_artist,
        "base_key_index": base_key_index,
        "artists": set(by_artist.keys()),
        "titles": set(by_title.keys()),
    }
    
    return index, diag

# ----- Playlist XML Writer -----
def write_playlist_xml(playlist_name, track_ids):
    """Write minimal iTunes/Music XML containing just playlist entries."""
    plist = {
        "Major Version": 1,
        "Minor Version": 1,
        "Application Version": "13.0",
        "Features": 5,
        "Show Content Ratings": True,
        "Tracks": {},  # empty; we rely on IDs already in library
        "Playlists": [{
            "Name": playlist_name,
            "Playlist ID": 1,
            "Playlist Persistent ID": "0000000000000001",
            "All Items": True,
            "Playlist Items": [{"Track ID": int(tid)} for tid in track_ids]
        }]
    }
    
    out = OUT_DIR / f"{playlist_name}.xml"
    with open(out, "wb") as fh:
        plistlib.dump(plist, fh, sort_keys=False)
    return out

# ----- Track Matching -----
def best_match(row, index):
    """Find best matching track in Music library for a CSV row."""
    # Read Exportify's exact column names
    title = row.get("Track Name", "") or row.get("track_name", "")
    artists_field = row.get("Artist Name(s)", "") or row.get("artist_name(s)", "")
    album = row.get("Album Name", "") or row.get("album_name", "")
    isrc = (row.get("ISRC") or "").strip()
    
    # Handle multiple artists - take first as primary, but also try full string
    primary_artist = artists_field.split(",")[0].strip() if artists_field else ""
    
    # Extract duration from Exportify's column name
    ms = None
    if row.get("Track Duration (ms)"):
        try: 
            ms = float(row["Track Duration (ms)"])
        except: 
            pass
    
    # Try multiple key variants for better matching
    # 1. Primary artist (first in list) with title
    # 2. Full artist string with title  
    # 3. With album if available
    keys_to_try = []
    
    # Primary artist keys
    if primary_artist:
        prim_keys = key_variants(primary_artist, title, album, ms)
        keys_to_try.extend(prim_keys)
    
    # Full artist string keys (if different from primary)
    if artists_field and artists_field != primary_artist:
        full_keys = key_variants(artists_field, title, album, ms)
        keys_to_try.extend(full_keys)
    
    # Try each key variant
    for (k, csv_secs) in keys_to_try:
        if k in index:
            # Choose candidate with closest duration
            cands = sorted(
                index[k], 
                key=lambda c: 0 if dur_close(c["secs"], csv_secs) else abs((c["secs"] or 0)-(csv_secs or 0))
            )
            
            for c in cands:
                if dur_close(c["secs"], csv_secs):
                    return c
            
            # Fallback to first candidate if no duration match
            if cands:
                return cands[0]
    
    return None

# ----- Diagnostic Functions -----
def summarize_candidate(c):
    """Format a candidate track for display."""
    return f"[id {c['track_id']}] {c['artist']} — {c['title']} · {c['album']} · {c['secs']}s"

def simplify_title_normed(s: str) -> str:
    """Strip common qualifiers: remastered, feat, radio edit, etc."""
    s = re.sub(r"\s*-\s*(remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended|deluxe).*$", "", s, flags=re.I)
    s = re.sub(r"\s*\((?:feat\.?|featuring|with|remaster(?:ed)?|mono|stereo|radio edit|edit|version|live|extended).*?\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*\[.*?\]\s*$", "", s)
    return s.strip()

def diagnose_row(row, index, diag, playlist_name, emitted_count):
    """Print why a row failed to match and suggest nearest candidates."""
    title = row.get("Track Name", "") or row.get("track_name", "")
    artists_field = row.get("Artist Name(s)", "") or row.get("artist_name(s)", "")
    album = row.get("Album Name", "") or row.get("album_name", "")
    primary_artist = artists_field.split(",")[0].strip() if artists_field else ""
    
    ms = None
    if row.get("Track Duration (ms)"):
        try: 
            ms = float(row["Track Duration (ms)"])
        except: 
            pass
    
    # Get all keys that would be tried
    cand_keys = []
    if primary_artist:
        cand_keys += key_variants(primary_artist, title, album, ms)
    if artists_field and artists_field != primary_artist:
        cand_keys += key_variants(artists_field, title, album, ms)
    
    na, nt, nal = norm(primary_artist), norm(title), norm(album)
    secs = round(float(ms)/1000.0) if ms else None
    
    print(f"\n— DEBUG({playlist_name}) #{emitted_count+1}")
    print(f"  CSV: {artists_field} — {title} · {album} · {secs}s")
    print(f"  norm: {na} — {nt} · {nal}")
    print("  keys tried (order):")
    
    for k, _ in cand_keys:
        present = k in index
        count = len(index.get(k, []))
        print(f"    • {k} -> {'HIT' if present else 'MISS'} ({count} candidate{'s' if count!=1 else ''})")
        
        if present:
            # Show top 3 candidates nearest in duration
            cands = index[k]
            def score(c):
                if secs is None or c['secs'] is None: 
                    return 9999
                return abs(c['secs'] - secs)
            
            for c in sorted(cands, key=score)[:min(3, len(cands))]:
                d = None if secs is None or c['secs'] is None else c['secs'] - secs
                print(f"        - {summarize_candidate(c)}  Δt={d}")
    
    # If all keys missed, provide suggestions
    if all(k not in index for k, _ in cand_keys):
        title_hits = diag['by_title'].get(nt, [])
        if title_hits:
            print("  Exact title exists in library (different artist/album):")
            for c in title_hits[:SUGGESTIONS_PER_ROW]:
                print(f"    · {summarize_candidate(c)}")
        
        artist_bucket = diag['by_artist'].get(na, [])
        if artist_bucket:
            ratios = [(difflib.SequenceMatcher(None, nt, norm(c['title'])).ratio(), c) for c in artist_bucket]
            top = [c for r, c in sorted(ratios, key=lambda x: x[0], reverse=True) if r >= 0.70][:SUGGESTIONS_PER_ROW]
            if top:
                print("  Close titles for same artist (≥0.70):")
                for c in top:
                    print(f"    · {summarize_candidate(c)}")
        
        title_bucket = diag['by_title'].get(nt, [])
        if title_bucket:
            ratios = [(difflib.SequenceMatcher(None, na, norm(c['artist'])).ratio(), c) for c in title_bucket]
            top = [c for r, c in sorted(ratios, key=lambda x: x[0], reverse=True) if r >= 0.70][:SUGGESTIONS_PER_ROW]
            if top:
                print("  Close artists for same title (≥0.70):")
                for c in top:
                    print(f"    · {summarize_candidate(c)}")
        
        # What-if: strip qualifiers from title
        alt_nt = simplify_title_normed(nt)
        if alt_nt != nt:
            base_key = f"{na}|{alt_nt}"
            bucket = diag['base_key_index'].get(base_key, [])
            if bucket:
                print("  Would match if title qualifiers removed:")
                for c in bucket[:SUGGESTIONS_PER_ROW]:
                    print(f"    · {summarize_candidate(c)}")

# ----- Main Processing -----
def process_playlists(index, diag=None):
    """Process all CSV playlists and convert to XML."""
    if not CSV_DIR.exists():
        print(f"\nERROR: CSV directory '{CSV_DIR}' not found!")
        print("\nTo export your Spotify playlists:")
        print("1. Go to https://exportify.net")
        print("2. Log in with Spotify")
        print("3. Click 'Export All'")
        print("4. Extract the ZIP to a folder called 'spotify_csv' in this directory")
        sys.exit(1)
    
    csv_files = list(glob.glob(str(CSV_DIR / "*.csv")))
    
    if not csv_files:
        print(f"\nNo CSV files found in {CSV_DIR}/")
        print("Make sure you've extracted your Exportify export to this directory.")
        sys.exit(1)
    
    print(f"\nFound {len(csv_files)} CSV playlists to process")
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
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
        
        with open(csv_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = best_match(row, index)
                if m:
                    track_ids.append(m["track_id"])
                else:
                    # Use the exact column names from Exportify
                    artist = row.get("Artist Name(s)", "") or row.get("artist_name(s)", "")
                    title = row.get("Track Name", "") or row.get("track_name", "")
                    unmatched_in_playlist.append((artist, title))
                    unmatched_report.append((pl_name, artist, title))
                    unmatched_by_artist[norm(artist)] += 1
                    unmatched_by_title[norm(title)] += 1
                    
                    # Debug diagnostics if enabled
                    if DEBUG and diag and debug_emitted < DEBUG_LIMIT_PER_PLAYLIST:
                        diagnose_row(row, index, diag, pl_name, debug_emitted)
                        debug_emitted += 1
        
        if track_ids:
            out = write_playlist_xml(pl_name, track_ids)
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
        rpt = OUT_DIR / "_unmatched.tsv"
        with open(rpt, "w", encoding="utf-8") as fh:
            fh.write("Playlist\tArtist\tTrack\n")
            for pl, a, t in unmatched_report:
                fh.write(f"{pl}\t{a}\t{t}\n")
        print(f"\n{len(unmatched_report)} unmatched tracks saved to {rpt}")
    
    print(f"\nConversion complete!")
    print(f"• Processed {processed_count}/{len(csv_files)} playlists")
    print(f"• Total tracks: {total_tracks}")
    print(f"• Output directory: {OUT_DIR}/")
    
    if unmatched_report:
        print(f"\nTop unmatched artists:")
        for a, cnt in unmatched_by_artist.most_common(10):
            print(f"  {a}  ×{cnt}")
        print(f"\nTop unmatched titles:")
        for t, cnt in unmatched_by_title.most_common(10):
            print(f"  {t}  ×{cnt}")
    
    return processed_count

# ----- Main Entry Point -----
def main():
    print("CSV to Music.app XML Playlist Converter")
    print("=" * 40)
    
    # Build index from Music Library
    index, diag = build_music_index()
    
    # Process all CSV playlists
    processed = process_playlists(index, diag)
    
    if processed > 0:
        print("\nNext steps:")
        print("1. Open Music.app")
        print("2. Go to File → Library → Import Playlist...")
        print("3. Select all XML files in 'music_playlists_xml/' (Cmd+A)")
        print("4. Click 'Open'")
        print("5. Your playlists will appear in Music.app and djay Pro!")
        print("\nNote: Only tracks already in your Music library will be added.")

if __name__ == "__main__":
    main()