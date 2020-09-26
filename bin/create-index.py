import argparse
import time
from Helper.config_file_handler import ConfigFileHelper
from Helper.database_helper import DataBaseHelper
from Helper.directory_indexer import DirectoryIndexer

# create parser
parser = argparse.ArgumentParser()

# add arguments to the parser
parser.add_argument("cfg_file")

# parse the arguments
args = parser.parse_args()

# get the path to the configuration file
config_file_path = args.cfg_file

cfg = ConfigFileHelper(config_file_path)
cfg.read_config_file()

database = DataBaseHelper()
database.drop_files_table()
database.create_table()

# start a timer for measuring the indexing time
start = time.time()

indexer = DirectoryIndexer(cfg.return_folders())
indexer.scan_directories_and_insert(database=database)

end = time.time()

print("\n\nElapsed time: " + str((end - start)/60.0) + "minutes")
print("Files indexed: " + str(database.count_all_rows_from_table()[0]))
