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
from pathlib import Path
from unidecode import unidecode

# ----- Configuration -----
LIB_XML = Path("MusicLibrary.xml")                  # exported from Music.app
CSV_DIR = Path("spotify_csv")                       # folder of Exportify CSVs  
OUT_DIR = Path("music_playlists_xml")               # where we write playlist XMLs
DUR_TOLERANCE_SEC = 3                               # ± seconds allowed when matching
USE_ALBUM_IN_MATCH = True                           # tighten matching when album is present

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
        
        for (k, _csv_secs) in variants:
            bucket = index.setdefault(k, [])
            bucket.append({
                "track_id": int(tid),
                "pid": td.get("Persistent ID"),
                "secs": secs,
                "artist": artist,
                "title": title,
                "album": album,
            })
    
    print(f"Indexed {local_count} local tracks from Music library")
    return index

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
    # Try various column name variants
    artist = row.get("Artist Name") or row.get("Artist") or row.get("artist") or ""
    title = row.get("Track Name") or row.get("track_name") or row.get("Track") or row.get("track") or ""
    album = row.get("Album Name") or row.get("album") or ""
    
    # Try to extract duration
    ms = None
    dur_fields = ["Duration (ms)", "duration_ms", "Duration"]
    for df in dur_fields:
        if df in row and str(row[df]).strip():
            try:
                ms = float(row[df])
                break
            except: 
                pass
    
    cand_keys = key_variants(artist, title, album, ms)
    
    for (k, csv_secs) in cand_keys:
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
            return cands[0]
    
    return None

# ----- Main Processing -----
def process_playlists(index):
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
    processed_count = 0
    total_tracks = 0
    
    for csv_path in csv_files:
        pl_name = Path(csv_path).stem
        track_ids = []
        unmatched_in_playlist = []
        
        with open(csv_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = best_match(row, index)
                if m:
                    track_ids.append(m["track_id"])
                else:
                    artist = row.get("Artist Name") or row.get("Artist") or row.get("artist") or ""
                    title = row.get("Track Name") or row.get("Track") or row.get("track") or row.get("title") or ""
                    unmatched_in_playlist.append((artist, title))
                    unmatched_report.append((pl_name, artist, title))
        
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
    
    return processed_count

# ----- Main Entry Point -----
def main():
    print("CSV to Music.app XML Playlist Converter")
    print("=" * 40)
    
    # Build index from Music Library
    index = build_music_index()
    
    # Process all CSV playlists
    processed = process_playlists(index)
    
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