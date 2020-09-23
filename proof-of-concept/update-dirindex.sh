#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

function main()
{
    pushd "$SCRIPTPATH" > /dev/null
    git pull
    popd >/dev/null
}

main


