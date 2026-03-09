#!/bin/bash
# Prepare sqlite-vec extension for bundling with desktop app
# Copies vec0 library from Python package to ~/.aivectormemory/

set -e

DEST="$HOME/.aivectormemory"
mkdir -p "$DEST"

VEC_PATH=$(python3 -c "import sqlite_vec; print(sqlite_vec.loadable_path())" 2>/dev/null || true)

if [ -z "$VEC_PATH" ]; then
    echo "sqlite-vec not found. Install it: pip install sqlite-vec"
    exit 1
fi

# Find the actual file (with platform extension)
if [ -f "${VEC_PATH}.dylib" ]; then
    cp "${VEC_PATH}.dylib" "$DEST/vec0.dylib"
    echo "Copied vec0.dylib to $DEST/"
elif [ -f "${VEC_PATH}.so" ]; then
    cp "${VEC_PATH}.so" "$DEST/vec0.so"
    echo "Copied vec0.so to $DEST/"
elif [ -f "${VEC_PATH}.dll" ]; then
    cp "${VEC_PATH}.dll" "$DEST/vec0.dll"
    echo "Copied vec0.dll to $DEST/"
elif [ -f "$VEC_PATH" ]; then
    cp "$VEC_PATH" "$DEST/vec0"
    echo "Copied vec0 to $DEST/"
else
    echo "vec0 extension file not found at $VEC_PATH"
    exit 1
fi

echo "sqlite-vec ready for desktop app"
