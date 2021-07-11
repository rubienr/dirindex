#!/usr/bin/env bash
SCRIPT_NAME=`basename "$0"`
SCRIPT_RELPATH=`realpath -s $0`
#SCRIPT_PATH='$(dirname ${BASH_SOURCE[0]})'
SCRIPT_PATH=`dirname $SCRIPT_RELPATH`
# Note:  EXECUTE is 0 if script is sourced, EXECUTE is 1 if executed
(return 0 2>/dev/null) && EXECUTE="0" || EXECUTE="1"

function main() 
{
  local config_name="$1"
  if [ "x$config_name" == "x" ] ; then
    config_file="example_config"
  fi

   /bin/python3 $SCRIPT_PATH/bin/evaluate-unique.py --configuration_file $SCRIPT_PATH/configurations/${config_name}.cfg
}

if [ "x$EXECUTE" == "x1" ] ; then
    main "${@}" # pass all command line arguments (as strings)
fi

# usage: evaluate-index.sh config-name-wo-extension
