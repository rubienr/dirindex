import os
from configparser import ConfigParser


# Helper class that handles opening and managing the configuration file that contains the folders to be
# indexed by the tool
class ConfigFileHelper:

    def __init__(self, config_file_path):
        self._config_file_path = config_file_path
        self._config_file = None
        self._folders = None
        self._database_file_path = None

    ##################################################################################################

    def read_config_file(self):
        print("[CONFIGURATION LOAD START]")
        # sanity check on the provided config file path
        if self._config_file_path is not None:
            print("Loading configuration file: \'{}\'".format(self._config_file_path))
        else:
            raise ValueError("Please provide the path to the configuration file.")

        if not os.path.isfile(self._config_file_path):
            raise ValueError("Provided configuration file path does not exist or it is a folder")

        config = ConfigParser(allow_no_value=True)
        config.read_file(open(self._config_file_path, mode='r'))

        if not config.has_option("DEFAULT", "database_file_path"):
            print(
                "'database_file_path' not set in the loaded configuration. Database file will be placed in "
                "the same directory as the script, with the default name 'database.db'")
        self._database_file_path = config.get("DEFAULT", "database_file_path")
        if self._database_file_path is None:
            self._database_file_path = os.getcwd() + "/database.db"

        if os.path.isdir(self._database_file_path[0:self._database_file_path.rfind('/')]):
            print("WARNING: 'database_file_path' - Loaded path does not exist. Using default values.")

        if not config.has_section("paths"):
            raise ValueError("'[paths]' section not present in the loaded configuration")
        if not config.has_option("paths", "folders"):
            raise ValueError("'[paths]' section does not contain 'folders' parameter")

        provided_paths = config.get("paths", "folders").split('\n')
        provided_paths = list(filter(None, provided_paths))  # Remove all white spaces
        provided_paths = list(dict.fromkeys(provided_paths))  # Remove duplicates

        # Sanity check on loaded paths
        for folder in provided_paths:
            if not os.path.isdir(folder):
                print(f"{folder} does not exist. Will be skipped.")
                continue
            self._folders.append(folder)

        print("Successfully loaded configuration file.")
        print("Printing loaded values: ")
        print(f"Database location: \'{self._database_file_path}\'")
        print("Folders:")
        print(self._folders)
        print("[CONFIGURATION LOAD END]")

    ##################################################################################################

    def get_folders(self):
        return self._folders

    ##################################################################################################

    def get_database_path(self):
        return self._database_file_path

