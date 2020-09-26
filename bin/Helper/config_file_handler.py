import os


# Helper class that handles opening and managing the configuration file that contains the folders to be
# indexed by the tool
class ConfigFileHelper:
    def __init__(self, config_file_path):
        self._config_file_path = config_file_path
        self._config_file = None
        self._folders = None

    ##################################################################################################
    def read_config_file(self):
        # sanity check on the provided config file path
        if self._config_file_path is not None:
            print("Loading configuration file: {}".format(self._config_file_path))
        else:
            raise ValueError("Please provide the path to the configuration file.")

        if not os.path.isfile(self._config_file_path):
            raise ValueError("Provided configuration file path does not exist or it is a folder")

        # finally open the file and get the folder paths
        try:
            self._config_file = open(self._config_file_path)
            self._folders = self._config_file.read().splitlines()
            print(self._folders)
        finally:
            self._config_file.close()

    ##################################################################################################
    def return_folders(self):
        return self._folders
