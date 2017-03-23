tool-aws
========

[![Build Status](https://travis-ci.org/geoadmin/tool-aws.svg?branch=master)](https://travis-ci.org/geoadmin/tool-aws)

### Macro CMDs for managing AWS resources

## Installation

`$ pip install tool_aws`

## Usage

`$ s3rm --profile ${PROFILE_NAME} --bucket-name ${BUCKET_NAME} --prefix ${PATH}`

## Setup in dev mode

`$ virtualenv .venv`
`$ source .venv/bin/activate`
`$ pip install -e .`
`$ pip install -r dev-requirements.txt`


To launch the tests:

`$ nosetests tests/`

### Style

Control styling:

`$ flake8 tool_aws/ tests/`

Autofix mistakes:

`$ find tool_aws/* tests/* -type f -name '*.py' -print | xargs autopep8 --in-place --aggressive --aggressive --verbose`

