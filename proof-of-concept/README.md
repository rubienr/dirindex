**DEPRECATED** - just a imple prove of concept, but not fast, realized with shell tools


# Yet Another Directory Indexing Tool

Purpose of this project is to get a rather simple overview of all files in folder so that folder can be compared to a other folder.
It should help to consolidate same named files if they differ.

**Non aim:** 
* it is not fully automated yet
* it is not consolidation tool but rather an assistent
* it is not the fastest tool: 
  * 20k files on SSD take about 6 minutes
  * 10k files on ext USB 3.0 HDD takes about 9 minutes 

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

### Archive Index

`compress-index.sh`

Archives the `./index/` folder to `index-<date>.tar.gz`.
