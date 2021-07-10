import os
# from blake2b import blake2b
import hashlib
from .database_helper import DataBaseHelper
from .file_type import FileType
import datetime

##################################################################################################


def calculate_hash(file_path: str, block_size: int = 10240):
    """
    Helper function that calculates the hash of a file/folder.
    If the given path is a folder: the hash of the string absolute path will be calculated
    If the given path is a file: the hash of the binary content will be calculated
    :param file_path:
    :param block_size:
    :return: 
    """

    # TODO raoul - cleanup
    """
    h = blake2b.compress(digest_size=15)

    if os.path.isdir(file_path):
        h.update(file_path.encode())
    else:
        with open(file_path, 'rb') as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(BLOCK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
    return h.hexdigest()
    """

    hash_sum = hashlib.md5()

    if os.path.isdir(file_path):
        hash_sum.update(file_path.encode())
    else:
        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(block_size), b""):
                hash_sum.update(block)

    return hash_sum.hexdigest()


##################################################################################################

class DirectoryIndexer:
    """
    Helper class that indexes all the folders found in self._directory_list.
    """
    def __init__(self, directory_list: [str], hash_block_size: int = 1024):
        self._directory_list = directory_list  # type: [str]
        self.files_found_in_directories = []  # type: [FileType]
        self._hash_block_size = hash_block_size  # type: int

    ##################################################################################################

    def get_file_count_in_configured_folders(self):
        count = 0
        for folder in self._directory_list:
            for root_directory, sub_dir_list, file_list in os.walk(folder):
                for _file in file_list:
                    # count files in top level in the current directory
                    count += 1
                for file in sub_dir_list:
                    # count files in subdirectories
                    if os.path.isfile(os.path.join(root_directory, file)):
                        count += 1
        return count

    ##################################################################################################

    def scan_directories_and_insert(self, database: DataBaseHelper):
        """
        This function triggers the directory indexing, and inserts all the files in the provided database
        :param database:
        :return:
        """
        print("\n[INDEXING START]")
        print("Indexing files. This might take a few minutes. Please wait... ", end='')
        print()
        self._index_folders()
        count = len(self.files_found_in_directories)
        if count > 0:
            database.insert_files_in_both_databases(self.files_found_in_directories)
        print(f"DONE! {count} files indexed.")
        print("[INDEXING END]")

    ##################################################################################################

    def _index_folders(self):
        """
        This function indexes all the directories found in self._directory_list. The output is a list of
        FileType objects.
        :return: int Number of files indexed
        """
        for folder in self._directory_list:
            for root_directory, sub_dir_list, file_list in os.walk(folder):
                # TODO - process multithreaded
                for file in file_list:
                    # index files in top level in the current directory
                    self._generate_file_information(root_directory, relative_path_with_name=file)
                for file in sub_dir_list:
                    # index files in subdirectories
                    if os.path.isfile(os.path.join(root_directory, file)):
                        self._generate_file_information(root_directory, relative_path_with_name=file)

    ##################################################################################################

    def _generate_file_information(self, root_directory: str, relative_path_with_name: str):
        """
        Helper function that creates a FileType object, fills in all the fields and inserts that object in the
        indexed files list.
        :param root_directory: absolute path of the root directory (current directory being indexed)
        :param relative_path_with_name: relative path of the file (relative to the root directory)
        :return:
        """
        file_absolute_path = os.path.join(root_directory, relative_path_with_name)
        relative_path = "root" if relative_path_with_name.rfind('/') == -1 \
            else relative_path_with_name[:relative_path_with_name.rfind('/')]

        # folder_absolute_path = os.path.join(root_directory, relative_path) if relative_path != "root" \
        #     else root_directory

        file_name_and_extension = file_absolute_path.split('/')[-1]
        file_size_kb = os.stat(file_absolute_path).st_size

        creation_time = datetime.datetime.fromtimestamp(
            os.stat(file_absolute_path).st_ctime).strftime('%Y-%m-%d-%H:%M:%S')
        last_mod_time = datetime.datetime.fromtimestamp(
            os.stat(file_absolute_path).st_mtime).strftime('%Y-%m-%d-%H:%M:%S')

        self.files_found_in_directories.append(
            FileType(absolute_path=root_directory,
                     absolute_path_hash_tag=calculate_hash(file_absolute_path),
                     relative_path=relative_path,
                     filename=file_name_and_extension.split('.')[0],
                     file_extension=file_name_and_extension.split('.')[-1],
                     hash_tag=calculate_hash(file_absolute_path, self._hash_block_size),
                     file_size=file_size_kb / 1000.0,
                     creation_time=creation_time,
                     last_modified_time=last_mod_time))