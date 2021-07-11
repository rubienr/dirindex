import os
from configparser import ConfigParser

##################################################################################################
from .databases_config_mixin import DatabasesConfigMixin
from .evaluation_config_mixin import EvaluationConfigMixin
from .hashing_config_mixin import HashingConfigMixin
from .path_config_mixin import PathConfigMixin


class IndexingConfiguration(DatabasesConfigMixin, PathConfigMixin, HashingConfigMixin, EvaluationConfigMixin):
    """
    helper class that handles opening and managing the configuration file that contains the folders to be indexed by
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

        DatabasesConfigMixin.__init__(self, self._parser)
        PathConfigMixin.__init__(self, self._parser)
        HashingConfigMixin.__init__(self, self._parser)
        EvaluationConfigMixin.__init__(self, self._parser)

    ##################################################################################################

    def read_config(self):
        """
        helper function that triggers the parsing of the configuration file placed at path: self._config_file_path
        :return: None
        """

        if self._config_file_path is not None:
            print("Loading configuration '{}' ...".format(self._config_file_path))
        else:
            raise ValueError("ERROR: Please provide the path to the configuration file ({})."
                             .format(self._config_file_path))

        if not os.path.isfile(self._config_file_path):
            raise ValueError("ERROR: Provided configuration file path does not exist or it is a folder ({})."
                             .format(self._config_file_path))

        self._parser.read_file(open(self._config_file_path, mode='r'))

        self.read_dbs_config()
        self.read_folder_config()
        self.read_hashing_config()
        self.read_evaluation_config()

        print("Loading configuration done.")


######################################################################################################

class EvaluationConfiguration(IndexingConfiguration, EvaluationConfigMixin):

    ##################################################################################################

    def __init__(self, config_file_path: str):
        self._config_file_path = config_file_path  # type: str
        self._parser = ConfigParser(allow_no_value=True)  # type: ConfigParser

        DatabasesConfigMixin.__init__(self, self._parser)
        PathConfigMixin.__init__(self, self._parser)
        HashingConfigMixin.__init__(self, self._parser)
        EvaluationConfigMixin.__init__(self, self._parser)

    ##################################################################################################

    def read_config(self):
        self.read_evaluation_config()
        IndexingConfiguration.read_config(self)

