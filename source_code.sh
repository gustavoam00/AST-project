#!/bin/bash
set -e

ZIP_NAME="source_code.zip"
QUERIES_TEST="queries"
DB="db"
SOURCE_DIR="src"
REDUCER="reducer"
MAIN_FILE="main.py"

# Clean up any existing zip file
[ -f "$ZIP_NAME" ] && rm "$ZIP_NAME"

# Check that required files exist
[ ! -d "$QUERIES_TEST" ] && echo "Directory '$QUERIES_TEST' not found!" && exit 1
[ ! -d "$SOURCE_DIR" ] && echo "Directory '$SOURCE_DIR' not found!" && exit 1
[ ! -f "$MAIN_FILE" ] && echo "File '$MAIN_FILE' not found!" && exit 1
[ ! -f "$REDUCER" ] && echo "File '$REDUCER' not found!" && exit 1

# Add main.py and src/ folder into the zip
zip -r "$ZIP_NAME" "$MAIN_FILE" "$SOURCE_DIR" "$QUERIES_TEST" "$DB" "$REDUCER"

echo "✅ Created $ZIP_NAME with 'main.py' and '$SOURCE_DIR/'"
