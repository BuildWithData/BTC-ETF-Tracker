#! /bin/bash

echo "############################"
echo "Creating env variable PYTHONPATH..."
ROOT_DIRECTORY=$(cat config.yaml| grep root | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')
export PYTHONPATH=$ROOT_DIRECTORY/src

echo "############################"
DATA_PATH=$(cat config.yaml | grep data | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')
DB_PATH=$(cat config.yaml | grep db | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')

if [ -z "$DB_PATH" ]
then
    DB_PATH=$DATA_PATH/db
fi

echo "Setting up storage..."
mkdir -pv $DATA_PATH/raw

echo "Setting up db..."
mkdir -pv $DB_PATH
mkdir -pv $DATA_PATH/db/consumption
