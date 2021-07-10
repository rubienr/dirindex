#!/usr/bin/env bash
SCRIPT_NAME=`basename "$0"`
#SCRIPT_PATH='$(dirname ${BASH_SOURCE[0]})'
SCRIPT_RELPATH=`realpath -s $0`
SCRIPT_PATH=`dirname $SCRIPT_RELPATH`
# EXECUTE == 0 if script is sourced, == 1 if executed
(return 0 2>/dev/null) && EXECUTE="0" || EXECUTE="1"

function main() 
{
   /bin/python3 $SCRIPT_PATH/bin/create-index.py --cfg_file $SCRIPT_PATH/cfg/example_config.cfg
}

if [ "x$EXECUTE" == "x1" ] ; then
    main "${@}" # pass all command line arguments (as strings)
fi
