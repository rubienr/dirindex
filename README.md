# Yet Another Directory Indexing Tool

Purpose of this project is to get a rather simple overview of all files in several (rather similar) folder so that those folder can be compared with each other.
It should help to consolidate same named files if they differ or are missing.

**Non aim:** 
* it is not fully automated yet
* it is not consolidation tool but rather an assistent
* it is not the fastest tool: 
  * 20k files on SSD take about 6 minutes **TODO - TBD**
  * 10k files on ext USB 3.0 HDD takes about 9 minutes **TODO - TBD**

## Usage
**TODO**

### Index Folder
**TODO**
suggestion:
1. crate a file table for file attributes
  - path to index (as specified in the configuration file)
  - relative path to file
  - file name
  - file extension
  - creation time stamp
  - last modificaton time stamp
  - file size
  - md5sum or other hash
1. create a folder table (optional)
  - relative folder name
  - # of contained files (immediate files)
  - # of contained folder (immediate folder)
1. for each configured folder
  - recursively gather information of each file
  - recursively gather information of each folder (optional)

### Query Files
**TODO**
suggestion:
1. index all base folder
1. intersect all knwon files
1. define a folderquery (by absolute file-path or md5sum etc.) that lists all missing files of a base folder vs intersection of all files
