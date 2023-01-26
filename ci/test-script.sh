#!/usr/bin/env sh
set -e
echo "Running Flake8"
flake8 --tee --output-file=build/flake8.txt
echo "Running Tests"
coverage run
coverage report
coverage xml
