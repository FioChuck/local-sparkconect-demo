#!/bin/bash

# Configuration
VENV_PATH="$HOME/.pyenv/versions/3.11.10/envs/spark-connect"


# Install dependencies using the specific venv pip
$VENV_PATH/bin/pip install -r requirements.txt
