#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-tote-bag-data-transformation
REGION = eu-west-2 # Could potentially remove
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################

# terraform setup

# call inside terraform working directory

# build-lambda-layer-dependencies:
# 	$(call execute_in_env, $(PIP) install -r ../requirements-lambda.txt -t ../dependencies/python)
#     $(call execute_in_env, mkdir -p ../packages/dependencies)
#     $(call execute_in_env, cd ../dependencies)
#     $(call execute_in_env, zip ../packages/dependencies/dependencies.zip -r python/)
# 	$(call execute_in_env, cd ../terraform)

# build-lambda-layer-utils:
# 	$(call execute_in_env, mkdir -p ../utils_layer/python/utils)
#     $(call execute_in_env, mkdir -p ../packages/utils)
#     $(call execute_in_env, cp -r ../utils/ ../utils_layer/python/utils/)
#     $(call execute_in_env, cd ../utils_layer)
# 	$(call execute_in_env, zip ../packages/utils/utils.zip -r python/)

# terraform-setup: build-lambda-layer-dependencies build-lambda-layer-utils

################################################################################################################

# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, black & coverage)
dev-setup: bandit black coverage

# Build / Run

## Run the security test (bandit)
security-test:
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./utils/*.py  ./src/*/*.py ./tests/*/*.py)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src --cov=utils tests/)
	
## Run all checks
run-checks: security-test run-black unit-test check-coverage

all: requirements dev-setup run-checks 
