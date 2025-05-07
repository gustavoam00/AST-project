#!/bin/bash

# generates the bug-reproducers.zip from the bugs/ folder.

set -e

ZIP_NAME="bug-reproducers.zip"
SOURCE_DIR="bugs"

[ -f "$ZIP_NAME" ] && rm "$ZIP_NAME"

[ ! -d "$SOURCE_DIR" ] && echo "Directory '$SOURCE_DIR' not found!" && exit 1

(cd "$SOURCE_DIR" && zip -r "../$ZIP_NAME" .)

echo "Created $ZIP_NAME without top-level '$SOURCE_DIR' directory."
