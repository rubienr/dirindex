import argparse
from datetime import timedelta
from timeit import default_timer as timer
from typing import List, Union

from helper.config_file_handler import EvaluationConfiguration
from helper.database_helper import EvaluationDataBases, UniqueFileFolderEvaluator, ExpectedFolderStructureEvaluator


##################################################################################################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configuration_file",
                        required=True, nargs=1, type=str, dest="cfg_file",
                        metavar="Configuration",
                        help="Path to the configuration file to be loaded. "
                             "An example Configuration can be found in the 'configurations' folder.")
    args = parser.parse_args()

    cfg = EvaluationConfiguration(args.cfg_file[0])
    cfg.read_config()

    database = EvaluationDataBases(cfg.public_index_db_cfg, cfg.evaluation_db_cfg)
    evaluators = [UniqueFileFolderEvaluator(database), ExpectedFolderStructureEvaluator(database)]

    try:
        start_timestamp = timer()
        [e.evaluate() for e in evaluators]
    finally:
        database.close()

    print("\nOverall elapsed time {}.".format(timedelta(seconds=timer() - start_timestamp)))


##################################################################################################


if __name__ == "__main__":
    main()
