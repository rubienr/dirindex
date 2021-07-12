import os
from configparser import ConfigParser


##################################################################################################

class DatabaseConfigMixin(object):

    ##################################################################################################

    def __init__(self, config_parser: ConfigParser,
                 section_name: str = "private_index_db",
                 field_name: str = "database_file_path",
                 default_db_name: str = "private_database.sqlite"):
        self._parser = config_parser  # type: ConfigParser

        self._section_name = section_name
        self._field_name = field_name
        self._database_file_path = ""
        self._default_database_name = default_db_name

    ##################################################################################################

    def read_config(self):
        self.__handle_database_path()

        print("[{}]".format(self._section_name))
        print("\t{} = '{}'".format(self._field_name, self._database_file_path))

    ##################################################################################################

    def get_database_path(self):
        return self._database_file_path

    ##################################################################################################

    def __handle_database_path(self):
        self._database_file_path = get_configured_db_file_path(
            self._parser,
            self._section_name,
            self._field_name,
            self._default_database_name)


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
