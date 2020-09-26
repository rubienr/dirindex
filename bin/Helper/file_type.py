
# Class that holds the relevant information about a specific file.
class FileType:
    def __init__(self, absolute_path="", relative_path="", filename="", file_extension="", hash_tag="", file_size="",
                 last_modified_time="", creation_time=""):
        self.absolute_path = absolute_path
        self.relative_path = relative_path
        self.filename = filename
        self.file_extension = file_extension
        self.hash_tag = hash_tag
        self.file_size_kb = file_size
        self.last_modified_time = last_modified_time
        self.creation_time = creation_time

    def print_file_metadata(self):
        print("Absolute path: {} - Relative path: {} | Filename: {} | File Extension {} | Hash: {} | "
              "Size: {} kb | Last modified time: {} | Creation time: {} "
              .format(self.absolute_path, self.relative_path, self.filename, self.file_extension, self.hash_tag,
                      self.file_size_kb, self.last_modified_time, self.creation_time))
