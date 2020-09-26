import sqlite3
from .file_type import FileType


##################################################################################################

# Helper class that manages the database related functionality
class DataBaseHelper:

    def __init__(self, db_name="dirindex.db", table_name="directories"):
        self.absolute_path = "absolute_path"  # type: str
        self.relative_path = "relative_path"  # type: str
        self.filename = "filename"  # type: str
        self.file_extension = "file_extension"  # type :str
        self.hash_tag = "hash_tag"  # type: str
        self.file_size_kb = "file_size_kb"  # type: str
        self.last_modification_time = "last_modification_time"  # type: str
        self.creation_time = "creation_time"  # type: str

        self._database_name = db_name
        self._db_connection = sqlite3.connect(self._database_name)
        self._table_name = table_name  # type:str
        self._current_cursor = self._db_connection.cursor()

    ##################################################################################################

    def create_table(self):

        # Create table
        self._current_cursor = self._db_connection.cursor()
        self._current_cursor.execute("CREATE TABLE IF NOT EXISTS " +
                                     self._table_name + " ( " +
                                     self.absolute_path + " text NOT NULL, " +
                                     self.relative_path + " text NOT NULL, " +
                                     self.filename + " text NOT NULL, " +
                                     self.file_extension + " text, " +
                                     self.hash_tag + " text NOT NULL, " +
                                     self.file_size_kb + " real NOT NULL, " +
                                     self.last_modification_time + " text NOT NULL, " +
                                     self.creation_time + " text NOT NULL, " +
                                     "PRIMARY KEY ( " + self.absolute_path + " , " + self.hash_tag + " ));")
        self._db_connection.commit()

    ##################################################################################################
    # Helper function that inserts a single file in the database
    def insert_file_in_table(self, file: FileType):
        if file is None:
            return
        self._current_cursor.execute("INSERT INTO " + self._table_name + " VALUES {}"
                                     .format(self.convert_file_type_to_sql_entry(file=file)))
        self._db_connection.commit()

    ##################################################################################################
    # Helper function that accepts a list of file objects to be inserted in the database with a single insert command.
    def insert_files_in_table(self, files: [FileType]):
        if len(files) is 0:
            return
        for file in files:
            self.insert_file_in_table(file)

    ##################################################################################################

    def select_file_by_absolute_path(self, absolute_file_path: str):
        if absolute_file_path == "":
            return ""
        self._current_cursor.execute("SELECT * FROM " + self._table_name + " WHERE " + self.absolute_path + " = ? ",
                                     [absolute_file_path])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def select_file_by_hash_tag(self, hash_tag: str):
        if hash_tag == "":
            return ""
        self._current_cursor.execute("SELECT * FROM " + self._table_name + " WHERE " + self.hash_tag + " = ? ",
                                     [hash_tag])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def print_files_table(self):
        self._current_cursor.execute("SELECT * FROM " + self._table_name)
        print(self._current_cursor.fetchall())

    ##################################################################################################

    def count_all_rows_from_table(self):
        self._current_cursor.execute("SELECT COUNT(*) FROM " + self._table_name)
        return self._current_cursor.fetchone()

    ##################################################################################################

    def drop_files_table(self):
        self._current_cursor.execute("DROP TABLE IF EXISTS " + self._table_name)

    ##################################################################################################

    def convert_file_type_to_sql_entry(self, file: FileType):
        return "('{}','{}','{}','{}','{}','{}','{}','{}')" \
                .format(file.absolute_path,
                        file.relative_path,
                        file.filename,
                        file.file_extension,
                        file.hash_tag,
                        file.file_size_kb,
                        file.last_modified_time,
                        file.creation_time)
