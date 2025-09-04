# Spotify to Music.app Playlist Converter

Convert Spotify playlists (exported as CSV via Exportify) to iTunes XML format for import into Music.app. These playlists will then appear in djay Pro.

## Quick Start

### Prerequisites
- macOS with Music.app
- `uv` installed (`brew install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Setup

1. **Export your Music library:**
   - Open Music.app
   - File ’ Library ’ Export Library...
   - Save as `MusicLibrary.xml` in this directory

2. **Export Spotify playlists:**
   - Visit https://exportify.net
   - Log in with Spotify
   - Click "Export All"
   - Extract the ZIP to `spotify_csv/` folder in this directory

3. **Run the converter:**
   ```bash
   uv run python csv_to_music_xml.py
   ```

4. **Import to Music.app:**
   - Open Music.app
   - File ’ Library ’ Import Playlist...
   - Select all files in `music_playlists_xml/` (Cmd+A)
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

## Configuration

Edit these variables in `csv_to_music_xml.py`:

```python
LIB_XML = Path("MusicLibrary.xml")     # Music library export
CSV_DIR = Path("spotify_csv")          # Spotify CSV folder
OUT_DIR = Path("music_playlists_xml")  # Output directory
DUR_TOLERANCE_SEC = 3                  # Duration matching tolerance
USE_ALBUM_IN_MATCH = True              # Use album for matching
```

## Troubleshooting

- **No matches found:** Check that track names/artists match between Spotify and your Music library
- **Unmatched tracks:** Review `_unmatched.tsv` in the output directory
- **djay Pro doesn't show playlists:** Restart djay Pro or refresh its library

## Notes

- This tool works with local files only (no Apple Music streaming content)
- DRM-protected files won't work for DJ mixing in djay Pro
- The script uses fuzzy matching with accents removed and flexible duration matching