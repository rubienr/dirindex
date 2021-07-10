import os
import sys
import hashlib
from typing import List
from .database_helper import DataBaseHelper
from .file_type import FileType
import datetime
import concurrent
import concurrent.futures


##################################################################################################


def calculate_hash(file_path: str, block_size: int = 10240, hash_content=True):
    """
    Helper function that calculates the hash of a file/folder.
    If the given path is a folder: the hash of the string absolute path will be calculated
    If the given path is a file: the hash of the binary content will be calculated
    :param file_path: complete path to file with extension
    :param block_size:
    :param hash_content: Hash the file content (True) or the file_path string (False)
    :return: 
    """

    hash_sum = hashlib.md5()

    if not hash_content:
        hash_sum.update(file_path.encode())
    if os.path.isdir(file_path):
        assert False
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

    def __init__(self, directory_list: [str], hash_file_block_size: int = 1024, hash_file_name_block_size: int = 1024):
        self._directory_list = directory_list  # type: List[str]
        self.files_found_in_directories = []  # type: List[FileType]
        self._hash_file_name_block_size = hash_file_name_block_size  # type: int
        self._hash_file_block_size = hash_file_block_size  # type: int

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

        print("Indexing done.")
        return count

    ##################################################################################################

    def scan_directories_and_insert(self, database: DataBaseHelper):
        """
        This function triggers the directory indexing, and inserts all the files in the provided database
        :param database:
        :return:
        """
        print("\n[INDEXING START]")
        print("Indexing files. This might take a few minutes. Please wait... ")
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

        print("Start indexing ...")

        num_total_processed_files = 0
        num_total_folders = 0
        num_total_results = 0
        num_total_tasks = 0

        with concurrent.futures.ProcessPoolExecutor() as executor:
            for root_directory in self._directory_list:
                fs = []
                num_processed_files = 0
                num_folders = 0
                num_results = 0
                num_tasks = 0
                print("Indexing folder {} ...".format(root_directory))
                sys.stdout.flush()
                for rel_dir, dirs, files in os.walk(root_directory):
                    rel_dir = os.path.relpath(rel_dir, root_directory)

                    # index files in top level in the current directory
                    for file_name in files:
                        num_tasks += 1
                        fs.append(executor.submit(
                            _generate_file_information, root_directory, rel_dir, file_name,
                            self._hash_file_name_block_size, self._hash_file_block_size))

                    num_processed_files += len(files)
                    num_folders += len(dirs)

                for future in concurrent.futures.as_completed(fs):
                    assert not future.cancelled()
                    self.files_found_in_directories.append(future.result())
                    num_results += 1

                print("\tProcessed {} files in {} folders.".format(num_processed_files, num_folders))
                sys.stdout.flush()
                num_total_processed_files += num_processed_files
                num_total_folders += num_folders

                num_total_results = num_results
                num_total_tasks = num_tasks

        print("Indexed overall {o} files in {f} folders ({i} items total)."
              .format(o=num_total_processed_files,
                      f=num_total_folders,
                      i=(num_total_processed_files + num_total_folders)))

    ##################################################################################################


def _generate_file_information(root_directory: str, relative_directory: str, file_name: str,
                               hash_file_name_block_size: int,
                               hash_file_block_size: int):
    """
    Helper function that creates a FileType object, fills in all the fields and inserts that object in the
    indexed files list.
    :param root_directory: absolute path to the root directory (current directory being indexed)
    :param relative_directory: relative path of the file (from the root_directory)
    :param file_name: file name with extension
    :param hash_file_name_block_size:
    :param hash_file_block_size::
    :return:
    """

    folder_absolute_path = os.path.join(root_directory, relative_directory)
    file_absolute_path = os.path.join(folder_absolute_path, file_name)

    file_size_kb = os.stat(file_absolute_path).st_size
    creation_time = datetime.datetime.fromtimestamp(
        os.stat(file_absolute_path).st_ctime).strftime('%Y-%m-%d-%H:%M:%S')
    last_mod_time = datetime.datetime.fromtimestamp(
        os.stat(file_absolute_path).st_mtime).strftime('%Y-%m-%d-%H:%M:%S')

    return FileType(
        absolute_path=root_directory,
        absolute_path_hash_tag=calculate_hash(file_absolute_path, hash_file_name_block_size, hash_content=False),
        relative_path=relative_directory,
        filename=file_name.split('.')[0],
        file_extension=file_name.split('.')[-1],
        hash_tag=calculate_hash(file_absolute_path, hash_file_block_size, hash_content=True),
        file_size=file_size_kb / 1000.0,
        creation_time=creation_time,
        last_modified_time=last_mod_time
    )
