#!/bin/bash

set -e

source config/pysaprklabs.env

conda remove --name ${PYSPARK_LABS_VENV} --all
conda create --name ${PYSPARK_LABS_VENV} python=3.7
conda activate ${PYSPARK_LABS_VENV}

pip install --upgrade pip wheel setuptools numpy
pip install -r requirements.txt
