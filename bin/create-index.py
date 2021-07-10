import argparse
import time
import sys
from Helper.config_file_handler import ConfigFileHelper
from Helper.database_helper import DataBaseHelper
from Helper.directory_indexer import DirectoryIndexer

##################################################################################################


def main():
    # create parser
    parser = argparse.ArgumentParser()

    # add arguments to the parser
    parser.add_argument("--cfg_file")

    # parse the arguments
    args = parser.parse_args()

    # get the path to the configuration file
    config_file_path = args.cfg_file

    cfg = ConfigFileHelper(config_file_path)
    cfg.read_config_file()

    if cfg.get_folders() is None:
        sys.stdout.write("No valid paths to index.")
        sys.exit(-1)

    indexer = DirectoryIndexer(cfg.get_folders(), cfg.get_hash_file_block_size(), cfg.get_hash_file_name_block_size())
    database = DataBaseHelper(secret_db_path=cfg.get_secret_database_path(),
                              public_db_path=cfg.get_public_database_path())

    # start a timer for measuring the indexing time
    start = time.time()

    indexer.scan_directories_and_insert(database=database)
    # database.create_secret_result_table()

    end = time.time()
    print("Elapsed time: " + str((end - start) / 60.0) + " minutes")

##################################################################################################


if __name__ == "__main__":
    main()
