import argparse
from timeit import default_timer as timer
import sys
from datetime import timedelta
from helper.config_file_handler import ConfigFileHelper
from helper.database_helper import DataBaseHelper
from helper.directory_indexer import DirectoryIndexer


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
    cfg.read_config()

    if cfg.get_folders() is None:
        sys.stdout.write("No valid paths to index.")
        sys.exit(-1)

    indexer = DirectoryIndexer(cfg, cfg)
    database = DataBaseHelper(cfg)

    try:
        start_timestamp = timer()
        indexer.scan_directories_and_insert(database=database)
        # database.create_secret_result_table()
    finally:
        database.close_connections()

    print("\nOverall elapsed time {}.".format(timedelta(seconds=timer() - start_timestamp)))


##################################################################################################


if __name__ == "__main__":
    main()
