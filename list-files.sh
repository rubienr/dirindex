#!/bin/bash

# $1 ... folder to traverse recursively
# $2 ... delimiter to separate columns
function traverse_files()
{
    directory="$@"
    delimiter=" "

    SAVEIFS=$IFS
    IFS=$(echo -en "\n\b")
    
    for filepath in `find $directory -type f` ; do

      f_details=`ls -lgGo --time-style=full-iso --tabsize=0 "$filepath"`

      f_size=`echo $f_details | cut -d' ' -f3`
      f_date=`echo $f_details | cut -d' ' -f4`
      f_time=`echo $f_details | cut -d' ' -f5`
      f_zone=`echo $f_details | cut -d' ' -f6`
      f_name=`echo $f_details | cut -d' ' -f7`

      f_sum=`md5sum $filepath`

      echo "${f_sum}${delimiter}${f_size}${delimiter}${f_date}${delimiter}${f_time}${delimiter}${f_zone}"

    done
    IFS=$SAVEIFS
}


function main()
{
    traverse_files "$@"
}

main $1
