#!/usr/bin/env sh
set -e
mkdir -p build
pip3 install --upgrade pip
pip3 install -e '.[testing]'
