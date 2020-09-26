import os
from pyblake2 import blake2b
from .database_helper import DataBaseHelper
from .file_type import FileType
import datetime

##################################################################################################

BLOCK_SIZE = 10240

##################################################################################################


class DirectoryIndexer:

    def __init__(self, directory_list):
        self._directory_list = directory_list

    ##################################################################################################

    def scan_directories_and_insert(self, database: DataBaseHelper):
        files_found_in_directories = []
        for folder in self._directory_list:
            for root_directory, sub_dir_list, file_list in os.walk(folder):
                for file_name in file_list:
                    file_absolute_path = os.path.join(root_directory, file_name)
                    file_size_kb = os.stat(file_absolute_path).st_size
                    creation_time = datetime.datetime.fromtimestamp(os.stat(file_absolute_path).st_ctime).strftime('%Y-%m-%d-%H:%M:%S')
                    last_mod_time = datetime.datetime.fromtimestamp(os.stat(file_absolute_path).st_mtime).strftime('%Y-%m-%d-%H:%M:%S')

                    files_found_in_directories.append(FileType(absolute_path=root_directory,
                                                               relative_path=file_name,
                                                               filename=file_name,
                                                               file_extension=file_name.split('.')[-1],
                                                               hash_tag=self.calculate_hash(file_absolute_path),
                                                               file_size=file_size_kb/1000.0,
                                                               creation_time=creation_time,
                                                               last_modified_time=last_mod_time))

        database.insert_files_in_table(files_found_in_directories)

    ##################################################################################################

    def calculate_hash(self, file_path):
        h = blake2b(digest_size=15)
        with open(file_path, 'rb') as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(BLOCK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
