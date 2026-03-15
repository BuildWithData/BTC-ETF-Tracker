#! /bin/bash

echo "############################"
echo "Creating env variable PYTHONPATH..."
ROOT_DIRECTORY=$(grep '^[[:space:]]*root:' config.yaml | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')
export PYTHONPATH=$ROOT_DIRECTORY/src

echo "############################"
DATA_PATH=$(grep '^[[:space:]]*data:' config.yaml | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')
DB_PATH=$(grep '^[[:space:]]*db:' config.yaml | cut -d : -f2 | sed 's/"//g' | sed 's/ //g')

if [ -z "$DB_PATH" ]
then
    DB_PATH=$DATA_PATH/db
fi

echo "Setting up storage..."
mkdir -pv $DATA_PATH/raw

echo "Setting up db..."
mkdir -pv $DB_PATH
mkdir -pv $DB_PATH/consumption
