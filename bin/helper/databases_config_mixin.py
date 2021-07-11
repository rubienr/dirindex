import os
from configparser import ConfigParser
from enum import Enum
from pathlib import Path


##################################################################################################

class DatabasesConfigMixin(object):
    ##################################################################################################

    class DatabaseType(Enum):
        PRIVATE = 0,
        PUBLIC = 1

    ##################################################################################################
    SECTION_NAME = "databases"

    PRIVATE_DB_CONFIG_FIELD_NAME = "private_database_file_path"
    PUBLIC_DB_CONFIG_FIELD_NAME = "public_database_file_path"

    DEFAULT_PRIVATE_DB_NAME = "private_database.sqlite"
    DEFAULT_PUBLIC_DB_NAME = "public_database.sqlite"

    ##################################################################################################
    def __init__(self, config_parser: ConfigParser):
        self._parser = config_parser  # type: ConfigParser

        self._private_database_file_path = ""  # type: str
        self._public_database_file_path = ""  # type: str

    ##################################################################################################

    def read_dbs_config(self):
        self.__handle_database_path(self.DatabaseType.PRIVATE)
        self.__handle_database_path(self.DatabaseType.PUBLIC)

        print("[{}]".format(DatabasesConfigMixin.SECTION_NAME))
        print("\t{} = '{}'".format(DatabasesConfigMixin.PRIVATE_DB_CONFIG_FIELD_NAME, self._private_database_file_path))
        print("\t{} = '{}'".format(DatabasesConfigMixin.PUBLIC_DB_CONFIG_FIELD_NAME, self._public_database_file_path))

    ##################################################################################################

    def get_private_database_path(self):
        return self._private_database_file_path

    ##################################################################################################

    def get_public_database_path(self):
        return self._public_database_file_path

    ##################################################################################################

    def __handle_database_path(self, database_type: DatabaseType):
        """
        Private helper function that can handle DB paths as listed in the config file.
        It checks if the respective parameter is set. If not, the default database name/location will be used.
        """
        if database_type == DatabasesConfigMixin.DatabaseType.PRIVATE:
            config_field = DatabasesConfigMixin.PRIVATE_DB_CONFIG_FIELD_NAME
            db_default_path = DatabasesConfigMixin.DEFAULT_PRIVATE_DB_NAME
        elif database_type == DatabasesConfigMixin.DatabaseType.PUBLIC:
            config_field = DatabasesConfigMixin.PUBLIC_DB_CONFIG_FIELD_NAME
            db_default_path = DatabasesConfigMixin.DEFAULT_PUBLIC_DB_NAME
        else:
            assert False

        actual_db_path = get_configured_db_file_path(self._parser, DatabasesConfigMixin.SECTION_NAME, config_field,
                                                     db_default_path)

        if database_type == DatabasesConfigMixin.DatabaseType.PRIVATE:
            self._private_database_file_path = actual_db_path
        if database_type == DatabasesConfigMixin.DatabaseType.PUBLIC:
            self._public_database_file_path = actual_db_path


######################################################################################################

def get_configured_db_file_path(config_parser: ConfigParser, section_name: str,
                                config_field: str, db_default_path: str):
    if not config_parser.has_option(section_name, config_field):
        print(
            "WARNING: '{}' - not set in the loaded configuration. Using default path.".format(config_field))
        actual_db_path = os.getcwd() + "/{}".format(db_default_path)
    else:
        actual_db_path = config_parser.get(section_name, config_field)

    if actual_db_path is None:
        pass
    elif not os.path.isdir(actual_db_path[0:actual_db_path.rfind('/')]):
        print("WARNING: '{}' - Loaded path does not exist. Using default path.".format(config_field))
        actual_db_path = os.getcwd() + "/{}".format(db_default_path)

    return actual_db_path
