import sqlite3
from .file_type import FileType


##################################################################################################

# Helper class that manages the database related functionality
class DataBaseHelper:

    def __init__(self, db_name="dirindex.db", all_files_table_name="directories", unique_files_table_name="unique_files"):
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
        self._all_files_table_name = all_files_table_name  # type:str
        self._unique_files_table_name = unique_files_table_name  # type:str
        self._current_cursor = self._db_connection.cursor()

    ##################################################################################################

    def create_table(self):

        # Create table
        self._current_cursor = self._db_connection.cursor()
        self._current_cursor.execute("CREATE TABLE IF NOT EXISTS " +
                                     self._all_files_table_name + " ( " +
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
        self._current_cursor.execute("INSERT INTO " + self._all_files_table_name + " VALUES {}"
                                     .format(self.convert_file_type_to_sql_entry(file=file)))
        self._db_connection.commit()

    ##################################################################################################
    # Helper function that accepts a list of file objects to be inserted in the database with a single insert command.
    def insert_files_in_table(self, files: [FileType]):
        if len(files) == 0:
            return
        for file in files:
            self.insert_file_in_table(file)

    ##################################################################################################

    def select_file_by_absolute_path(self, absolute_file_path: str):
        if absolute_file_path == "":
            return ""
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name + " WHERE " + self.absolute_path + " = ? ",
                                     [absolute_file_path])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def select_file_by_hash_tag(self, hash_tag: str):
        if hash_tag == "":
            return ""
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name + " WHERE " + self.hash_tag + " = ? ",
                                     [hash_tag])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def print_files_table(self):
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name)
        print(self._current_cursor.fetchall())

    ##################################################################################################

    def count_all_rows_from_table(self):
        self._current_cursor.execute("SELECT COUNT(*) FROM " + self._all_files_table_name)
        return self._current_cursor.fetchone()

    ##################################################################################################

    def drop_files_table(self):
        self._current_cursor.execute("DROP TABLE IF EXISTS " + self._all_files_table_name)

    ##################################################################################################

    def create_table_of_unique_files(self):
        self._current_cursor.execute("CREATE TABLE IF NOT EXISTS (" + self._unique_files_table_name +
                                     "AS ( " + "SELECT DISTINCT " + self.filename + " , " + self.hash_tag
                                     + " FROM " + self._all_files_table_name + " ));")
        self._db_connection.commit()

    ##################################################################################################

    # This function creates a table with all the possible combinations between all unique files
    # and all unique folders. This is used as an image of what the folder structure should look like
    def create_cartesian_product(self):
        print("This will create the full folder structure")

    ##################################################################################################

    def get_missing_files_from_all_folders(self):
        """
            SELECT DISTINCT cartesian_product.*
                FROM   cartesian_product
                WHERE  NOT EXISTS (SELECT *
                                   FROM   directories
                                   WHERE  cartesian_product.hash_tag = directories.hash_tag
                                          AND cartesian_product.absolute_path = directories.absolute_path)
            :return:
            """
        print("Here we will give back a list of folders with their missing files.")



    ##################################################################################################

    def select_unique_files_by_hash(self):
        self._current_cursor.execute("SELECT DISTINCT " + self.filename + " , " + self.hash_tag + " FROM " + self._all_files_table_name)
        return self._current_cursor.fetchall()

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
