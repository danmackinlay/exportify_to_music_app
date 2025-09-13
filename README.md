# Spotify to Music.app Playlist Converter

Convert Spotify playlists (exported as CSV via Exportify) to iTunes XML format for import into Music.app. These playlists will then appear in e.g. djay Pro.

## Quick Start

### Prerequisites

- macOS with Music.app
- `uv` installed (`brew install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Setup

1. **Export your Music library:**
   - Open Music.app
   - File > Library > Export Library...
   - Save as `MusicLibrary.xml` in the `data/` directory

2. **Export Spotify playlists:**
   - Visit https://exportify.net
   - Log in with Spotify
   - Click "Export All"
   - Extract the ZIP to `data/spotify_csv/` folder

3. **Run the converter:**
   ```bash
   uv run python csv_to_music_xml.py
   ```

4. **Import to Music.app:**
   - Open Music.app
   - File > Library > Import Playlist...
   - Select all files in `data/music_playlists_xml/` (Cmd+A)
   - Click Open

5. **Use in djay Pro:**
   - Open djay Pro
   - Your playlists will appear under the Music/iTunes source
   - If not visible, refresh the library or restart djay Pro

## How it Works

The script:

1. Indexes your local Music library from the exported XML
2. Reads each Spotify CSV and matches tracks by artist, title, album, and duration
3. Creates minimal XML files containing only playlist references to existing tracks
4. Only tracks already in your Music library will be added (streaming-only content is ignored)

## Advanced Usage

The converter includes advanced matching algorithms and command-line options:

### Basic Options

```bash
# Show help and all available options
uv run python csv_to_music_xml.py --help

# Use custom file locations
uv run python csv_to_music_xml.py --library ~/MusicLibrary.xml --csv-dir ~/spotify_exports --output ~/playlists

# Debug unmatched tracks
uv run python csv_to_music_xml.py --debug

# More detailed debugging (show 10 failures per playlist, 5 suggestions each)
uv run python csv_to_music_xml.py --debug --limit 10 --suggestions 5
```

### ISRC Matching (Exact Track Identification)

For the most accurate matching, enable ISRC-based identification:

```bash
# Enable smart ISRC matching with caching (recommended)
uv run python csv_to_music_xml.py --isrc

# Tune performance for large libraries
uv run python csv_to_music_xml.py --isrc --max-tag-reads 1000 --workers 16

# Skip album prefetching for faster startup
uv run python csv_to_music_xml.py --isrc --isrc-prefetch none

# Use custom cache location
uv run python csv_to_music_xml.py --isrc --isrc-cache ~/music_isrc_cache.sqlite
```

ISRC matching provides:
- **Exact track identification** when ISRCs are available
- **Persistent caching** - subsequent runs are near-instant
- **Smart prefetching** - only reads files for albums in your CSV exports
- **Concurrent processing** - multi-threaded for fast performance

### Matching Algorithm

The converter uses a layered matching approach (in priority order):

1. **ISRC exact match** - Zero ambiguity when available
2. **Album + disc/track number** - Precise matching for well-tagged libraries
3. **Title simplification** - Strips "remastered", "feat.", etc. automatically
4. **Fuzzy matching** - Token-set similarity for close matches
5. **Adaptive duration tolerance** - 2% tolerance for longer tracks

### Performance

- **Fast**: Indexes 18k+ tracks in seconds
- **Smart**: Only reads audio files when needed for ISRC matching
- **Cached**: Persistent SQLite cache prevents re-reading unchanged files
- **Concurrent**: Multi-threaded audio file processing

## Troubleshooting

- **No matches found:** Check that track names/artists match between Spotify and your Music library
- **Unmatched tracks:** Review `_unmatched.tsv` in the output directory
- **djay Pro doesn't show playlists:** Restart djay Pro or refresh its library

## Notes

- This tool works with local files only (no Apple Music streaming content)
- DRM-protected files won't work for DJ mixing in djay Pro
- The script uses fuzzy matching with accents removed and flexible duration matching