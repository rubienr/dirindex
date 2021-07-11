from configparser import ConfigParser
from .databases_config_mixin import get_configured_db_file_path


##################################################################################################

class EvaluationConfigMixin(object):
    ##################################################################################################

    SECTION_NAME = "evaluation"
    EVALUATION_DB_CONFIG_FIELD_NAME = "evaluation_database_file_path"
    DEFAULT_EVALUATION_DB_NAME = "evaluation_database.sqlite"

    ##################################################################################################
    def __init__(self, config_parser: ConfigParser):
        self._parser = config_parser  # type: ConfigParser
        self._evaluation_database_file_path = ""  # type: str

    ##################################################################################################

    def read_evaluation_config(self):
        self.__handle_evaluation_database_path()

        print("[{}]".format(EvaluationConfigMixin.SECTION_NAME))
        print("\t{} = '{}'".format(EvaluationConfigMixin.EVALUATION_DB_CONFIG_FIELD_NAME,
                                   self._evaluation_database_file_path))

    ##################################################################################################

    def get_evaluation_database_path(self):
        return self._evaluation_database_file_path

    ##################################################################################################

    def __handle_evaluation_database_path(self):
        self._evaluation_database_file_path = get_configured_db_file_path(
            self._parser,
            EvaluationConfigMixin.SECTION_NAME,
            EvaluationConfigMixin.EVALUATION_DB_CONFIG_FIELD_NAME,
            EvaluationConfigMixin.DEFAULT_EVALUATION_DB_NAME)
