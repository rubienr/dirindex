import argparse
from datetime import timedelta
from timeit import default_timer as timer

from helper.config_file_handler import IndexingConfiguration
from helper.database_helper import DataBaseIndexHelper
from helper.directory_indexer import DirectoryIndexer


##################################################################################################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configuration_file",
                        required=True, nargs=1, type=str, dest="cfg_file",
                        metavar="Configuration",
                        help="Path to the configuration file to be loaded. "
                             "An example Configuration can be found in the 'configurations' folder.")
    args = parser.parse_args()

    cfg = IndexingConfiguration(args.cfg_file[0])
    cfg.read_config()

    indexer = DirectoryIndexer(cfg.paths_cfg, cfg.hashing_cfg)
    database = DataBaseIndexHelper(cfg.private_index_db_cfg, cfg.public_index_db_cfg)

    try:
        start_timestamp = timer()
        indexer.scan_directories_and_insert(database)
    finally:
        database.close()

    print("\nOverall elapsed time {}.".format(timedelta(seconds=timer() - start_timestamp)))


##################################################################################################


if __name__ == "__main__":
    main()
