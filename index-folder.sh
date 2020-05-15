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


function sanity_check() 
{
     # check configuration
    
     num_dirs="${#FOLDER_TO_INDEX[@]}"
     num_indices="${#FOLDER_INDEX_FILE[@]}"
     if [ $num_dirs -ne $num_indices ] ; then
         echo "failed to read configuration: number of directories ($num_dirs) do not match number of indices ($num_indices)"
         exit -1
     fi
    
    # check if folder are available
    
     for f in "${!FOLDER_TO_INDEX[@]}" ; do
        folder=${FOLDER_TO_INDEX[$f]}
        if [ ! -d $folder ] ; then
          echo "failed to locate folder '$folder'"
          exit -1
        fi
      done
}


function main()
{
    pushd "$SCRIPTPATH" > /dev/null
    source index-folder.cfg

    mkdir -p "index"
    pushd "index" > /dev/null

    sanity_check
    index_folder

    popd >/dev/null
    popd >/dev/null
}

main
