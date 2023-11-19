#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $# -eq 0 ]]
then
    HARNESS_DIR=harness
    APP_DIRS="silo"
else
    APP_DIRS=$@
fi

for dir in ${HARNESS_DIR} ${APP_DIRS}
do
    echo "Cleaning $dir"
    cd ${ROOT}/${dir}
    ./clean.sh 
    EXIT_CODE=$?
    if [[ $EXIT_CODE -ne 0 ]]
    then
        echo "WARNING: Cleaning $dir returned error status $EXIT_CODE"
    fi
    cd -
done
