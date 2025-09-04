# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spotify to Music.app playlist converter that:
- Converts Spotify playlists (exported as CSV via Exportify) to iTunes XML format
- Matches tracks between Spotify and local Music library using fuzzy matching
- Creates minimal XML playlists for import into Music.app, which then appear in djay Pro

## Key Commands

### Run the converter
```bash
uv run python csv_to_music_xml.py
```

### Debug mode (shows why tracks aren't matching)
```bash
DEBUG=1 uv run python csv_to_music_xml.py
DEBUG=1 DEBUG_LIMIT=3 SUGGESTIONS=5 uv run python csv_to_music_xml.py
```

### Install dependencies
```bash
uv pip install -r pyproject.toml
```

## Architecture & Key Components

### Main Script: `csv_to_music_xml.py`

**Core Workflow:**
1. `build_music_index()` - Parses Music.app's exported XML library and creates searchable index
2. `process_playlists()` - Iterates through Spotify CSV files
3. `best_match()` - Matches CSV tracks to Music library using multiple key variants
4. `write_playlist_xml()` - Generates minimal iTunes XML files for import

**Matching Strategy:**
- Uses normalized strings (lowercase, accent removal, whitespace collapse)
- Tries multiple key variants: artist|title|album, artist|title
- Duration matching with configurable tolerance (±3 seconds default)
- Handles multiple artists by trying primary artist and full artist string

**Debug Features:**
- `diagnose_row()` - Shows why tracks fail to match
- Suggests similar tracks based on title/artist fuzzy matching
- Identifies when removing qualifiers (remastered, feat., etc.) would help

### Data Flow
```
data/MusicLibrary.xml → Index in memory
data/spotify_csv/*.csv → Match against index → data/music_playlists_xml/*.xml
```

### Configuration (in script)
- `LIB_XML` - Path to Music library export
- `CSV_DIR` - Spotify CSV folder location  
- `OUT_DIR` - Output directory for XML playlists
- `DUR_TOLERANCE_SEC` - Duration matching tolerance
- `USE_ALBUM_IN_MATCH` - Whether to use album in matching