import sqlite3
from enum import Enum
from .file_type import FileType


##################################################################################################

# Helper class that manages the database related functionality
class DataBaseHelper:

    def __init__(self, db_name="dirindex.db", table_name="directories"):
        self.absolute_path = "absolute_path"  # type: str
        self.relative_path = "relative_path"  # type: str
        self.filename = "filename"  # type: str
        self.hash_tag = "hash_tag"  # type: str
        self.file_size = "file_size"  # type: str
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
                               self.relative_path + " text NOT NULL, " +
                               self.relative_path + " text NOT NULL, " +
                               self.filename + " text NOT NULL, " +
                               self.hash_tag + " text NOT NULL, " +
                               self.file_size + " real NOT NULL, " +
                               self.last_modification_time + " text NOT NULL, " +
                               self.creation_time + " text NOT NULL, " +
                               "PRIMARY KEY ( " + self.absolute_path + " , " + self.hash_tag + " ));")
        self._db_connection.commit()

    ##################################################################################################

    def insert_file_in_table(self, file: FileType):
        sql_insert_values = "('{}','{}','{}','{}','{}','{}','{}')" \
            .format(file.absolute_path,
                    file.relative_path,
                    file.filename,
                    file.hash_tag,
                    file.file_size,
                    file.last_modified_time,
                    file.creation_time)

        self._current_cursor.execute("INSERT INTO " + self._table_name + " VALUES {}".format(sql_insert_values))
        self._db_connection.commit()

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
