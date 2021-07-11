import os
from pathlib import Path
from configparser import ConfigParser


##################################################################################################

class DatabasesConfigMixin(object):
    ##################################################################################################
    SECTION_NAME = "databases"

    SECRET_DB_CONFIG_FIELD_NAME = "secret_database_file_path"
    PUBLIC_DB_CONFIG_FIELD_NAME = "public_database_file_path"

    DEFAULT_SECRET_DB_NAME = "secret_database.db"
    DEFAULT_PUBLIC_DB_NAME = "public_database.db"

    ##################################################################################################
    def __init__(self, config_parser: ConfigParser):
        self._parser = config_parser  # type: ConfigParser

        self._secret_database_file_path = ""  # type: str
        self._public_database_file_path = ""  # type: str

    ##################################################################################################

    def read_config(self):
        self._handle_database_path(secret=True)
        self._handle_database_path(secret=False)

        print("[{}]".format(DatabasesConfigMixin.SECTION_NAME))
        print("\t{} = '{}'".format(DatabasesConfigMixin.SECRET_DB_CONFIG_FIELD_NAME, self._secret_database_file_path))
        print("\t{} = '{}'".format(DatabasesConfigMixin.PUBLIC_DB_CONFIG_FIELD_NAME, self._public_database_file_path))

    ##################################################################################################

    def get_secret_database_path(self):
        return self._secret_database_file_path

    ##################################################################################################

    def get_public_database_path(self):
        return self._public_database_file_path

        ##################################################################################################

    def _handle_database_path(self, secret=True):
        """
        Private helper function that can handle both the public and the secret DB paths listed in the config file.
        It checks if the respective parameter is set. If not, the default database name/location will be used.
        :param secret: specifies which database (private or public) path shall be parsed
        :return:
        """
        if secret:
            config_field = DatabasesConfigMixin.SECRET_DB_CONFIG_FIELD_NAME
            db_default_path = DatabasesConfigMixin.DEFAULT_SECRET_DB_NAME
        else:
            config_field = DatabasesConfigMixin.PUBLIC_DB_CONFIG_FIELD_NAME
            db_default_path = DatabasesConfigMixin.DEFAULT_PUBLIC_DB_NAME

        if not self._parser.has_option(DatabasesConfigMixin.SECTION_NAME, config_field):
            print(
                "WARNING: '{}' - not set in the loaded configuration. Using default path.".format(config_field))
            actual_db_path = os.getcwd() + "/{}".format(db_default_path)
        else:
            actual_db_path = self._parser.get(DatabasesConfigMixin.SECTION_NAME, config_field)

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
