import sqlite3
from enum import Enum
from .file_type import FileType


##################################################################################################

# Helper class that manages the database related functionality
class DataBaseHelper:

    def __init__(self, db_name="dirindex.db", table_name="directories"):
        self._database_name = db_name
        self._db_connection = sqlite3.connect(self._database_name)
        self._table_name = table_name  # type:str

    ##################################################################################################

    def create_table(self):

        absolute_path = "absolute_path"  # type: str
        relative_path = "relative_path"  # type: str
        filename = "filename"  # type: str
        hash_tag = "hash_tag"  # type: str
        file_size = "file_size"  # type: str
        last_modification_time = "last_modification_time"  # type: str
        creation_time = "creation_time"  # type: str

        # Create table
        current_cursor = self._db_connection.cursor()
        current_cursor.execute("CREATE TABLE IF NOT EXISTS " +
                               self._table_name + " ( " +
                               relative_path + " text NOT NULL, " +
                               relative_path + " text NOT NULL, " +
                               filename + " text NOT NULL, " +
                               hash_tag + " text NOT NULL, " +
                               file_size + " real NOT NULL, " +
                               last_modification_time + " text NOT NULL, " +
                               creation_time + " text NOT NULL, " +
                               "PRIMARY KEY ( " + absolute_path + " , " + hash_tag + " ));")
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

        self._db_connection.execute("INSERT INTO {} VALUES {}".format(self._table_name, sql_insert_values))
        self._db_connection.commit()

    ##################################################################################################
