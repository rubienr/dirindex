# Class that holds the relevant information about a specific file.
class FileType:
    def __init__(self,
                 root_path="",
                 relative_path="",
                 filename="",
                 file_extension="",
                 file_mime_type="",
                 root_path_hash_tag="",
                 relative_path_hash_tag="",
                 filename_hash_tag="",
                 absolute_file_path_hash_tag="",
                 file_content_hash_tag="",
                 creation_time="",
                 last_modification_time="",
                 file_size=""):
        self.root_path = root_path
        self.relative_path = relative_path
        self.filename = filename
        self.file_extension = file_extension
        self.file_mime_type = file_mime_type

        self.root_path_hash_tag = root_path_hash_tag
        self.relative_path_hash_tag = relative_path_hash_tag
        self.filename_hash_tag = filename_hash_tag
        self.absolute_file_path_hash_tag = absolute_file_path_hash_tag
        self.file_content_hash_tag = file_content_hash_tag

        self.creation_time = creation_time
        self.last_modification_time = last_modification_time
        self.file_size = file_size
