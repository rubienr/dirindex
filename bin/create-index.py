import argparse
from Helper.config_file_handler import ConfigFileHelper

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


