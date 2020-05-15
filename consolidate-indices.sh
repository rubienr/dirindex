#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

function consolidate_indices()
{
    consolidated_index="combined.stats"
    indices=(`find . -type f -regex '.*[^.]*[^s]*[^t]*[^a]*[^t]*[^s]$'`)

    echo "glue indices together"
    
    for idx_file in "$indices" ; do
        echo "processing file $idx_file"
        while read line ; do
            
            # todo: make this parsing more performant
            checksum=`echo "$line" | cut -d' ' -f1`
            filepath=`echo "$line" | cut -d' ' -f3`
            file_name=`echo ${filepath##*/}`
            size=`echo "$line" | cut -d' ' -f4`
            date=`echo "$line" | cut -d' ' -f5`
            time=`echo "$line" | cut -d' ' -f6`
            utc_offset=`echo "$line" | cut -d' ' -f7`

            # todo: cannot specify var for output file name - ambiguous redirect
            echo "$file_name" >> consolidated-files.stats
            echo "$checksum" >> consolidated-checksums.stats
            echo "$file_name $checksum $size $date $time $utc_offset $filepath $idx_file" >> consolidated.stats
            
            # todo: cannot specify var for output file name
        done <${idx_file} 
        # >> "combined.stats"
    done
    
    echo "sorting indices"  
    sort -o consolidated-files.stats consolidated-files.stats
    sort -o consolidated-checksums.stats consolidated-checksums.stats
    sort -o consolidated.stats consolidated.stats
    
    # todo make uniq -c    
}

function main()
{
    pushd "$SCRIPTPATH" > /dev/null
    pushd "index" > /dev/null

    consolidate_indices 
    
    popd >/dev/null
    popd >/dev/null
}

main
