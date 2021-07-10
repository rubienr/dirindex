import os
from configparser import ConfigParser
from pathlib import Path

##################################################################################################

DEFAULT_SECTION_NAME = "DEFAULT"
PATHS_SECTION_NAME = "paths"
HASHING_SECTION_NAME = "hashing"
HASHING_BLOCK_SIZE_FIELD_NAME = "block_size"
DEFAULT_SECRET_DB_NAME = "secret_database.db"
DEFAULT_PUBLIC_DB_NAME = "public_database.db"
SECRET_DB_CONFIG_FIELD_NAME = "secret_database_file_path"
PUBLIC_DB_CONFIG_FIELD_NAME = "public_database_file_path"
FOLDER_LIST_FIELD_NAME = "folders"


##################################################################################################


class ConfigFileHelper:
    """
    Helper class that handles opening and managing the configuration file that contains the folders to be indexed by
    the tool.
    """

    ##################################################################################################

    def __init__(self, config_file_path: str):
        """
        Constructor that accepts the path to the configuration file as string.
        :param config_file_path: Path to the configuration file on the filesystem
        """
        self._config_file_path = config_file_path  # type: str
        self._parser = ConfigParser(allow_no_value=True)  # type: ConfigParser
        self._folders = []  # type: [str]
        self._secret_database_file_path = ""  # type: str
        self._public_database_file_path = ""  # type: str
        self._hash_block_size = 0  # type: int

    ##################################################################################################

    def read_config_file(self):
        """
        Helper function that triggers the parsing of the configuration file placed at path: self._config_file_path
        :return: None
        """
        print("[CONFIGURATION LOAD START]")
        # sanity check on the provided config file path
        if self._config_file_path is not None:
            print("Loading configuration file: '{}'".format(self._config_file_path))
        else:
            raise ValueError("ERROR: Please provide the path to the configuration file.")

        if not os.path.isfile(self._config_file_path):
            raise ValueError("ERROR: Provided configuration file path does not exist or it is a folder")

        self._parser.read_file(open(self._config_file_path, mode='r'))

        # Get the secret DB path listed in the config file and save it in self._secret_database_file_path
        self._handle_database_path(secret=True)

        # Get the public DB path listed in the config file and save it in self._public_database_file_path
        self._handle_database_path(secret=False)

        # Get all the folder paths listed in the config file and save them in self._folders member
        self._handle_paths_list()

        self._handle_hash_block_size()

        print("Successfully loaded configuration file.")
        print("Printing loaded values: ")
        print(f"Secret database location: \'{self._secret_database_file_path}\'")
        print(f"Public database location: \'{self._public_database_file_path}\'")

        print("{}:".format(PATHS_SECTION_NAME))
        print("  {}: {}".format(FOLDER_LIST_FIELD_NAME, self._folders))

        print("{}:".format(HASHING_SECTION_NAME))
        print("  {}: {}".format(HASHING_BLOCK_SIZE_FIELD_NAME, self._hash_block_size))

    print("[CONFIGURATION LOAD END]")

    ##################################################################################################

    def _handle_database_path(self, secret=True):
        """
        Private helper function that can handle both the public and the secret DB paths listed in the config file.
        It checks if the respective parameter is set. If not, the default database name/location will be used.
        :param secret: specifies which database (private or public) path shall be parsed
        :return:
        """
        if secret:
            config_field = SECRET_DB_CONFIG_FIELD_NAME
            db_default_path = DEFAULT_SECRET_DB_NAME
        else:
            config_field = PUBLIC_DB_CONFIG_FIELD_NAME
            db_default_path = DEFAULT_PUBLIC_DB_NAME

        if not self._parser.has_option(DEFAULT_SECTION_NAME, config_field):
            print(
                "WARNING: '{}' - not set in the loaded configuration. Using default path.".format(config_field))
            actual_db_path = os.getcwd() + "/{}".format(db_default_path)
        else:
            actual_db_path = self._parser.get(DEFAULT_SECTION_NAME, config_field)

        if actual_db_path is None:
            pass
        elif not os.path.isdir(actual_db_path[0:actual_db_path.rfind('/')]):
            print("WARNING: '{}' - Loaded path does not exist. Using default path.".format(config_field))
            actual_db_path = os.getcwd() + "/{}".format(db_default_path)

        if not os.path.isfile(actual_db_path):
            Path(actual_db_path).touch()

        if secret:
            self._secret_database_file_path = actual_db_path
        else:
            self._public_database_file_path = actual_db_path

    ##################################################################################################

    def _handle_paths_list(self):
        """
        Private helper function that verifies if the folder list exists in the provided config file. If the list exists,
        all the paths are read line by line and saved in the appropriate class member.
        This function also checks each path for correctness (i.e. if it exists or not). If any path provided does not
        exist, it will not be added to the class member.
        :return:
        """
        if not self._parser.has_section(PATHS_SECTION_NAME):
            raise ValueError("ERROR: '[{}]' section not present in the loaded configuration"
                             .format(PATHS_SECTION_NAME))
        if not self._parser.has_option(PATHS_SECTION_NAME, FOLDER_LIST_FIELD_NAME):
            raise ValueError("ERROR: '[{}}]' section does not contain '{}' parameter"
                             .format(PATHS_SECTION_NAME, FOLDER_LIST_FIELD_NAME))

        provided_paths = self._parser.get(PATHS_SECTION_NAME, FOLDER_LIST_FIELD_NAME).split('\n')
        provided_paths = list(filter(None, provided_paths))  # Remove all white spaces
        provided_paths = list(dict.fromkeys(provided_paths))  # Remove duplicates

        # Sanity check on loaded paths
        for folder in provided_paths:
            if not os.path.isdir(folder):
                print(f"WARNING: {folder} does not exist. Will be skipped.")
                continue
            self._folders.append(folder)

    ##################################################################################################

    def _handle_hash_block_size(self):
        if not self._parser.has_section(HASHING_SECTION_NAME):
            raise ValueError(
                "ERROR: '[{}]' section not present in the loaded configuration".format(HASHING_SECTION_NAME))
        if not self._parser.has_option(HASHING_SECTION_NAME, HASHING_BLOCK_SIZE_FIELD_NAME):
            raise ValueError("ERROR: '[{}}]' section does not contain 'folders' parameter".format(HASHING_SECTION_NAME))

        self._hash_block_size = int(self._parser.get(HASHING_SECTION_NAME, HASHING_BLOCK_SIZE_FIELD_NAME))

    ##################################################################################################

    def get_secret_database_path(self):
        return self._secret_database_file_path

    ##################################################################################################

    def get_public_database_path(self):
        return self._public_database_file_path

    ##################################################################################################

    def get_folders(self):
        return self._folders

    ##################################################################################################

    def get_hash_block_size(self):
        return self._hash_block_size
