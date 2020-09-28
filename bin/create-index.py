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

database = DataBaseHelper(db_path=cfg.get_database_path())
database.drop_all_tables_and_views()
database.create_table()

# start a timer for measuring the indexing time
start = time.time()

indexer = DirectoryIndexer(cfg.get_folders())
indexer.scan_directories_and_insert(database=database)
database.create_missing_files_from_all_folders_table()

print("\n[RESULTS]")
print("Missing files from folders:")
missing_files_from_folders = database.get_all_rows_from(database.result_table_name)
for entry in missing_files_from_folders:
    print(entry)

end = time.time()
print("Elapsed time: " + str((end - start)/60.0) + "minutes")

