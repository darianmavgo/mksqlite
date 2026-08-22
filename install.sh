#!/usr/bin/env bash
#
# Install script for mksqlite on macOS / Unix
# Installs mksqlite as a system-wide command in /usr/local/bin (or custom INSTALL_DIR)
#

set -euo pipefail

# Default installation directory
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BIN_NAME="mksqlite"

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Determine repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"

usage() {
    echo -e "${BLUE}Usage:${NC} $0 [options]"
    echo ""
    echo "Options:"
    echo "  --prefix <path>     Target installation directory (default: /usr/local/bin)"
    echo "  --uninstall         Remove mksqlite from installation directory"
    echo "  -h, --help          Show this help message"
    echo ""
    exit 0
}

# Parse command-line arguments
UNINSTALL=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --uninstall)
            UNINSTALL=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

if [ "$UNINSTALL" = true ]; then
    TARGET_PATH="${INSTALL_DIR}/${BIN_NAME}"
    echo -e "${YELLOW}Uninstalling ${BIN_NAME} from ${TARGET_PATH}...${NC}"
    if [ -f "${TARGET_PATH}" ]; then
        if [ -w "${INSTALL_DIR}" ]; then
            rm -f "${TARGET_PATH}"
        else
            sudo rm -f "${TARGET_PATH}"
        fi
        echo -e "${GREEN}✓ Successfully uninstalled ${BIN_NAME}.${NC}"
    else
        echo -e "${BLUE}No existing installation found at ${TARGET_PATH}.${NC}"
    fi
    exit 0
fi

echo -e "${BLUE}=== Installing ${BIN_NAME} system-wide ===${NC}"

# Check for Go
if ! command -v go >/dev/null 2>&1; then
    echo -e "${RED}Error: 'go' is not installed or not found in PATH.${NC}"
    echo "Please install Go from https://golang.org or via Homebrew: brew install go"
    exit 1
fi

GO_VERSION=$(go version)
echo -e "Using: ${GREEN}${GO_VERSION}${NC}"

# Create temporary build directory
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo -e "Building ${BIN_NAME}..."
(
    cd "${REPO_ROOT}"
    go build -ldflags="-s -w" -o "${TMP_DIR}/${BIN_NAME}" ./cmd/mksqlite
)

# Ensure install directory exists
if [ ! -d "${INSTALL_DIR}" ]; then
    echo -e "Creating directory ${INSTALL_DIR}..."
    if [ -w "$(dirname "${INSTALL_DIR}")" ]; then
        mkdir -p "${INSTALL_DIR}"
    else
        sudo mkdir -p "${INSTALL_DIR}"
    fi
fi

# Install binary
TARGET_PATH="${INSTALL_DIR}/${BIN_NAME}"
echo -e "Installing to ${GREEN}${TARGET_PATH}${NC}..."

if [ -w "${INSTALL_DIR}" ]; then
    install -m 755 "${TMP_DIR}/${BIN_NAME}" "${TARGET_PATH}"
else
    echo -e "${YELLOW}Administrator privileges required to write to ${INSTALL_DIR}.${NC}"
    sudo install -m 755 "${TMP_DIR}/${BIN_NAME}" "${TARGET_PATH}"
fi

# Check if INSTALL_DIR is in PATH
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo -e "${YELLOW}Warning: ${INSTALL_DIR} is not currently in your PATH.${NC}"
    echo "Add the following line to your ~/.zshrc or ~/.bash_profile:"
    echo "  export PATH=\"\$PATH:${INSTALL_DIR}\""
fi

echo ""
echo -e "${GREEN}✓ ${BIN_NAME} installed successfully!${NC}"
echo -e "Try running: ${BLUE}mksqlite --help${NC} or ${BLUE}mksqlite <input_file>${NC}"
