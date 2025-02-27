#!/bin/bash

set -e
rm -rf ../dependencies
mkdir -p ../dependencies/python
pip install -r ../requirements-lambda.txt -t ../dependencies/python
zip ../packages/dependencies/dependencies.zip -r dependencies/python
