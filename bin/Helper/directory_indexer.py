import os
from pyblake2 import blake2b
from .database_helper import DataBaseHelper
from .file_type import FileType
import datetime

##################################################################################################

BLOCK_SIZE = 10240


##################################################################################################

def calculate_hash(file_path):
    h = blake2b(digest_size=15)
    with open(file_path, 'rb') as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(BLOCK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


##################################################################################################

class DirectoryIndexer:

    def __init__(self, directory_list):
        self._directory_list = directory_list

    ##################################################################################################

    def get_file_count_in_configured_folders(self):
        count = 0
        for folder in self._directory_list:
            for root_directory, sub_dir_list, file_list in os.walk(folder):
                for file_name in file_list:
                    count += 1
        return count

    ##################################################################################################

    def scan_directories_and_insert(self, database: DataBaseHelper):
        print("\n[INDEXING START]")
        files_found_in_directories = []
        count = 0
        print("Indexing files ...", end='')
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
                                                               hash_tag=calculate_hash(file_absolute_path),
                                                               file_size=file_size_kb/1000.0,
                                                               creation_time=creation_time,
                                                               last_modified_time=last_mod_time))
                    count += 1
                    print('.', end='')
        if count > 0:
            # Printing results
            print(f" DONE! {count} files indexed.")
            print("Inserting files in the database ...", end='')
            database.insert_files_in_table(files_found_in_directories)
            print(" DONE!")
        print("[INDEXING END]")

    ##################################################################################################
