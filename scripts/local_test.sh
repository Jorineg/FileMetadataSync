#!/bin/bash
# Local test script for Mac
# Creates test data and runs single sync cycle

set -e
cd "$(dirname "$0")/.."

# Create test folder with some files
echo "Creating test data..."
mkdir -p test_data
echo "Hello World" > test_data/hello.txt
echo "Test file 2" > test_data/test2.txt
mkdir -p test_data/subfolder
echo "Nested file" > test_data/subfolder/nested.txt

echo "Test files created in test_data/"
ls -la test_data/
ls -la test_data/subfolder/

echo ""
echo "To run the sync:"
echo "  docker-compose -f docker-compose.local.yml up --build"
echo ""
echo "Or for a single sync cycle:"
echo "  docker-compose -f docker-compose.local.yml run --rm file-metadata-sync python scripts/sync_once.py"

