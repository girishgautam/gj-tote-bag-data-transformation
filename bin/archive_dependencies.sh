#!/bin/bash

set -e
rm -rf ../dependencies
mkdir -p ../dependencies/python
pip install -r ../requirements-lambda.txt -t ../dependencies/python
mkdir -p ../packages/dependencies/
zip /home/runner/work/de-tote-bag-data-transformation/de-tote-bag-data-transformation/packages/dependencies/dependencies.zip -r ../dependencies/python
ls -l /home/runner/work/de-tote-bag-data-transformation/de-tote-bag-data-transformation/packages/dependencies/dependencies.zip
chmod +r /home/runner/work/de-tote-bag-data-transformation/de-tote-bag-data-transformation/packages/dependencies/dependencies.zip

