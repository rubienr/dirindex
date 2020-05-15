#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

function index_folder()
{
    for f in "${!FOLDER_TO_INDEX[@]}" ; do

        folder=${FOLDER_TO_INDEX[$f]}
        index_file=${FOLDER_INDEX_FILE[$f]}
        num_files=`find "$folder" -type f | wc -l`

        echo "indexing '$folder' run at `date`" > ${index_file}.stats
        echo "files count $num_files" >> ${index_file}.stats

        echo "indexing '$folder' to ./index/$index_file ..."
        echo "found $num_files files"
        { time ./../list-files.sh "$folder"; } 1>./$index_file 2>>${index_file}.stats

    done
}


function main()
{
    pushd "$SCRIPTPATH" > /dev/null
    source index-folder.cfg

    mkdir -p "index"
    pushd "index" > /dev/null

    index_folder

    popd >/dev/null
    popd >/dev/null
}

main
