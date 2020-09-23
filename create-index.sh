#!/usr/bin/env bash
SCRIPT_NAME=`basename "$0"`
SCRIPT_PATH='$(dirname ${BASH_SOURCE[0]})'
# EXECUTE == 0 if script is sourced, == 1 if executed
(return 0 2>/dev/null) && EXECUTE="0" || EXECUTE="1"


function main() 
{
    $SCRIPT_PATH/bin/create-index.py --config $SCRIPT_PATH/cfg/create-index.cfg
}


if [ "x$EXECUTE" == "x1" ] ; then
    main "${@}" # pass all command line arguments (as strings)
fi
