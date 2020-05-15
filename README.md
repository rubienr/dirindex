# Yet Another Directory Indexint Tool

Purpose of this project is to get a rather simple overview of all files in folder so that it can be compared to a nother folder.
It may help to consolidate same named files if they differ.

**Non aim:** 

* it is not a consolidation tool
* it is not automated
* it may not be perfect but have bugs

## Usage

### Index Folder

`index-folder.sh` 
Reads the `index-folder.cfg` configuration and applies `list-files.sh` for each configured folder.
Indexed folder and meta data are stored to `./index/`.

### List Files

`list-files.sh <path>`
Prints all files found in this path recursively inclusive their checksum to std out. 
Columns are: 
* checksum, 
* file, 
* size, 
* date, 
* time, and 
* UCT offset.
