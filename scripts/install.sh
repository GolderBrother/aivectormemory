#!/bin/bash
# AIVectorMemory Desktop App Installer (macOS/Linux)
set -e

DEST="$HOME/.aivectormemory"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== AIVectorMemory Installer ==="
echo ""

# Create destination directory
mkdir -p "$DEST"

# Detect platform
OS="$(uname -s)"
case "$OS" in
    Darwin) PLATFORM="macOS" ; VEC_EXT="vec0.dylib" ;;
    Linux)  PLATFORM="Linux" ; VEC_EXT="vec0.so" ;;
    *)      echo "Unsupported platform: $OS" ; exit 1 ;;
esac

echo "Platform: $PLATFORM"

# Copy sqlite-vec extension
if [ -f "$SCRIPT_DIR/$VEC_EXT" ]; then
    cp "$SCRIPT_DIR/$VEC_EXT" "$DEST/$VEC_EXT"
    echo "Installed $VEC_EXT -> $DEST/$VEC_EXT"
else
    echo "Warning: $VEC_EXT not found in package, skipping"
fi

# macOS: copy .app to /Applications
if [ "$PLATFORM" = "macOS" ]; then
    APP_NAME="AIVectorMemory.app"
    if [ -d "$SCRIPT_DIR/$APP_NAME" ]; then
        echo ""
        echo "Install $APP_NAME to /Applications? [Y/n]"
        read -r answer
        if [ "$answer" != "n" ] && [ "$answer" != "N" ]; then
            cp -R "$SCRIPT_DIR/$APP_NAME" "/Applications/$APP_NAME"
            echo "Installed $APP_NAME -> /Applications/"
            # Remove quarantine attribute
            xattr -dr com.apple.quarantine "/Applications/$APP_NAME" 2>/dev/null || true
            echo "Cleared Gatekeeper quarantine flag"
        fi
    fi
fi

# Linux: create .desktop file
if [ "$PLATFORM" = "Linux" ]; then
    BIN_NAME="AIVectorMemory"
    if [ -f "$SCRIPT_DIR/$BIN_NAME" ]; then
        INSTALL_DIR="$HOME/.local/bin"
        mkdir -p "$INSTALL_DIR"
        cp "$SCRIPT_DIR/$BIN_NAME" "$INSTALL_DIR/$BIN_NAME"
        chmod +x "$INSTALL_DIR/$BIN_NAME"
        echo "Installed $BIN_NAME -> $INSTALL_DIR/"

        # Create .desktop file
        DESKTOP_DIR="$HOME/.local/share/applications"
        mkdir -p "$DESKTOP_DIR"
        cat > "$DESKTOP_DIR/aivectormemory.desktop" << EOF
[Desktop Entry]
Name=AIVectorMemory
Exec=$INSTALL_DIR/$BIN_NAME
Type=Application
Categories=Development;
Comment=AI Vector Memory Desktop App
EOF
        echo "Created desktop entry -> $DESKTOP_DIR/aivectormemory.desktop"
    fi
fi

echo ""
echo "Installation complete!"
