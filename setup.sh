#! /bin/bash

echo "############################"
echo "Creating env variable PYTHONPATH..."
ROOT_DIRECTORY=$(cat config.yaml| grep root | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')
export PYTHONPATH=$ROOT_DIRECTORY/src

echo "############################"
DATA_PATH=$(cat config.yaml | grep data | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')

echo "Setting up storage..."
mkdir -pv $DATA_PATH/raw
mkdir -pv $DATA_PATH/db
