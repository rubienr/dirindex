import os
from configparser import ConfigParser

######################################################################################################
from .databases_config_mixin import DatabaseConfigMixin
from .evaluation_config_mixin import EvaluationConfigMixin
from .hashing_config_mixin import HashingConfigMixin
from .path_config_mixin import PathConfigMixin


######################################################################################################

class IniConfigBase(object):

    ##################################################################################################

    def __init__(self, config_file_path: str):
        self._config_file_path = config_file_path  # type: str
        self.parser = ConfigParser(allow_no_value=True)  # type: ConfigParser
        self.configs = []

    ##################################################################################################

    def read_config(self):
        if self._config_file_path is not None:
            print("Loading configuration '{}' ...".format(self._config_file_path))
        else:
            raise ValueError("ERROR: Please provide the path to the configuration file ({})."
                             .format(self._config_file_path))

        if not os.path.isfile(self._config_file_path):
            raise ValueError("ERROR: Provided configuration file path does not exist or it is a folder ({})."
                             .format(self._config_file_path))

        self.parser.read_file(open(self._config_file_path, mode='r'))

        [c.read_config() for c in self.configs]

        print("Loading configuration done.")


######################################################################################################

class IndexingConfiguration(IniConfigBase):

    ##################################################################################################

    def __init__(self, config_file_path: str):
        super().__init__(config_file_path)

        self.public_index_db_cfg = DatabaseConfigMixin(
            self.parser,
            section_name="public_index_db",
            field_name="database_file_path",
            default_db_name="public_database.sqlite")
        self.private_index_db_cfg = DatabaseConfigMixin(
            self.parser,
            section_name="private_index_db",
            field_name="database_file_path",
            default_db_name="private_database.sqlite")
        self.paths_cfg = PathConfigMixin(self.parser)
        self.hashing_cfg = HashingConfigMixin(self.parser)

        self.configs = [self.public_index_db_cfg, self.private_index_db_cfg, self.paths_cfg, self.hashing_cfg]


######################################################################################################

class EvaluationConfiguration(IniConfigBase):

    ##################################################################################################

    def __init__(self, config_file_path: str):
        super().__init__(config_file_path)

        self.public_index_db_cfg = DatabaseConfigMixin(
            self.parser,
            section_name="public_index_db",
            field_name="database_file_path",
            default_db_name="public_database.sqlite")
        self.evaluation_db_cfg = DatabaseConfigMixin(
            self.parser,
            section_name="evaluation_db",
            field_name="database_file_path",
            default_db_name="evaluation_database.sqlite")

        self.configs = [self.public_index_db_cfg, self.evaluation_db_cfg]
