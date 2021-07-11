from configparser import ConfigParser


##################################################################################################

class HashingConfigMixin(object):
    ##################################################################################################

    SECTION_NAME = "hashing"
    BLOCK_SIZE_FIELD_NAME = "file_block_size"
    FILE_NAME_BLOCK_SIZE_FIELD_NAME = "file_name_block_size"

    ##################################################################################################

    def __init__(self, config_parser: ConfigParser):

        self._parser = config_parser  # type: ConfigParser

        self._hash_file_block_size = 0  # type: int
        self._hash_file_name_block_size = 0  # type: int

    ##################################################################################################

    def read_hashing_config(self):
        self.__handle_hash_file_block_size()
        self.__handle_hash_file_name_block_size()

        print("[{}]".format(HashingConfigMixin.SECTION_NAME))
        print("\t{} = '{}'".format(HashingConfigMixin.BLOCK_SIZE_FIELD_NAME, self._hash_file_block_size))
        print("\t{} = '{}'".format(HashingConfigMixin.FILE_NAME_BLOCK_SIZE_FIELD_NAME, self._hash_file_name_block_size))

    ##################################################################################################

    def get_hash_file_block_size(self):
        return self._hash_file_block_size

    ##################################################################################################

    def get_hash_file_name_block_size(self):
        return self._hash_file_name_block_size

    ##################################################################################################

    def __handle_hash_file_block_size(self):
        if not self._parser.has_section(HashingConfigMixin.SECTION_NAME):
            raise ValueError(
                "ERROR: '[{}]' section not present in the loaded configuration"
                    .format(HashingConfigMixin.SECTION_NAME))

        if not self._parser.has_option(HashingConfigMixin.SECTION_NAME,
                                       HashingConfigMixin.BLOCK_SIZE_FIELD_NAME):
            raise ValueError(
                "ERROR: '[{}]' section does not contain '{}' parameter"
                    .format(HashingConfigMixin.SECTION_NAME, HashingConfigMixin.BLOCK_SIZE_FIELD_NAME))

        self._hash_file_block_size = int(self._parser.get(
            HashingConfigMixin.SECTION_NAME, HashingConfigMixin.BLOCK_SIZE_FIELD_NAME))

    ##################################################################################################

    def __handle_hash_file_name_block_size(self):
        if not self._parser.has_section(HashingConfigMixin.SECTION_NAME):
            raise ValueError(
                "ERROR: '[{}]' section not present in the loaded configuration".format(HashingConfigMixin.SECTION_NAME))
        if not self._parser.has_option(HashingConfigMixin.SECTION_NAME, HashingConfigMixin.FILE_NAME_BLOCK_SIZE_FIELD_NAME):
            raise ValueError(
                "ERROR: '[{}}]' section does not contain '{}' parameter"
                    .format(HashingConfigMixin.SECTION_NAME, HashingConfigMixin.FILE_NAME_BLOCK_SIZE_FIELD_NAME))

        self._hash_file_name_block_size = int(
            self._parser.get(HashingConfigMixin.SECTION_NAME, HashingConfigMixin.FILE_NAME_BLOCK_SIZE_FIELD_NAME))
