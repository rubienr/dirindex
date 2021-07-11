import os
from configparser import ConfigParser


##################################################################################################

class PathConfigMixin(object):
    PATHS_SECTION_NAME = "paths"
    FOLDER_LIST_FIELD_NAME = "folders"

    ##################################################################################################
    def __init__(self, config_parser: ConfigParser):
        self._parser = config_parser  # type: ConfigParser
        self._folders = []  # type: [str]

    ##################################################################################################

    def read_config(self):
        self._handle_paths_list()

        print("[{}]".format(PathConfigMixin.PATHS_SECTION_NAME))
        print("\t{} = '{}'".format(PathConfigMixin.FOLDER_LIST_FIELD_NAME, self._folders))

    ##################################################################################################

    def get_folders(self):
        return self._folders

    ##################################################################################################

    def _handle_paths_list(self):

        if not self._parser.has_section(PathConfigMixin.PATHS_SECTION_NAME):
            raise ValueError("ERROR: '[{}]' section not present in the loaded configuration"
                             .format(PathConfigMixin.PATHS_SECTION_NAME))
        if not self._parser.has_option(PathConfigMixin.PATHS_SECTION_NAME, PathConfigMixin.FOLDER_LIST_FIELD_NAME):
            raise ValueError("ERROR: '[{}]' section does not contain '{}' parameter"
                             .format(PathConfigMixin.PATHS_SECTION_NAME, PathConfigMixin.FOLDER_LIST_FIELD_NAME))

        provided_paths = self._parser.get(PathConfigMixin.PATHS_SECTION_NAME,
                                          PathConfigMixin.FOLDER_LIST_FIELD_NAME).split('\n')
        provided_paths = list(filter(None, provided_paths))  # Remove all white spaces
        provided_paths = list(dict.fromkeys(provided_paths))  # Remove duplicates

        # Sanity check on loaded paths
        for folder in provided_paths:
            if not os.path.isdir(folder):
                print(f"WARNING: {folder} does not exist. Will be skipped.")
                continue
            self._folders.append(folder)
