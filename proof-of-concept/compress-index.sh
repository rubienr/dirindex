#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

function main()
{
    pushd "$SCRIPTPATH" > /dev/null

    if [ ! -d "./index" ] ; then
        echo "failed to locate './index' to compress"
        exit -1
    fi
   
    now=`date --iso-8601=seconds`
    archive="./index-${now}.tar.gz"
    tar -zcvf $archive ./index/
    echo "compressed ./index/ to $archive"
   
    popd >/dev/null
}

main


