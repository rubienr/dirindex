import sqlite3
from .file_type import FileType

##################################################################################################

UNIQUE_FILES_VIEW_NAME = "unique_files"
UNIQUE_FOLDERS_VIEW_NAME = "unique_folders"
EXPECTED_FOLDER_STRUCTURE_VIEW_NAME = "expected_folder_structure"


##################################################################################################


# Helper class that manages the database related functionality
class DataBaseHelper:

    def __init__(self, db_name="dirindex.db", all_files_table_name="directories"):
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
        self._current_cursor = self._db_connection.cursor()

    ##################################################################################################
    #
    # def __del__(self):
    #     self.drop_all_tables_and_views()

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
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name + " WHERE " +
                                     self.absolute_path + " = ? ", [absolute_file_path])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def select_file_by_hash_tag(self, hash_tag: str):
        if hash_tag == "":
            return ""
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name + " WHERE " +
                                     self.hash_tag + " = ? ", [hash_tag])
        return self._current_cursor.fetchone()

    ##################################################################################################

    def print_files_table(self):
        self._current_cursor.execute("SELECT * FROM " + self._all_files_table_name)
        print(self._current_cursor.fetchall())

    ##################################################################################################

    def count_all_rows_from_table_or_view(self, table_or_view=None):
        if table_or_view is None:
            table_or_view = self._all_files_table_name
        self._current_cursor.execute("SELECT COUNT(*) FROM " + table_or_view)
        return self._current_cursor.fetchone()

    ##################################################################################################

    def drop_files_table(self):
        self._current_cursor.execute("DROP TABLE IF EXISTS " + self._all_files_table_name)
        self._db_connection.commit()

    ##################################################################################################

    def drop_all_tables_and_views(self):
        self._current_cursor.execute("DROP TABLE IF EXISTS " + self._all_files_table_name)
        self._current_cursor.execute("DROP VIEW IF EXISTS " + UNIQUE_FILES_VIEW_NAME)
        self._current_cursor.execute("DROP VIEW IF EXISTS " + UNIQUE_FOLDERS_VIEW_NAME)
        self._current_cursor.execute("DROP VIEW IF EXISTS " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME)
        self._db_connection.commit()

    ##################################################################################################

    def create_view_of_unique_files(self):
        self._current_cursor.execute("CREATE VIEW " + UNIQUE_FILES_VIEW_NAME + " AS " + "SELECT DISTINCT "
                                     + self.hash_tag + " , " + self.filename
                                     + " FROM " + self._all_files_table_name)
        self._db_connection.commit()

    ##################################################################################################

    def create_view_of_unique_folders(self):
        self._current_cursor.execute("CREATE VIEW " + UNIQUE_FOLDERS_VIEW_NAME + " AS " + "SELECT DISTINCT "
                                     + self.absolute_path + " FROM " + self._all_files_table_name)
        self._db_connection.commit()

    ##################################################################################################

    # This function creates a table with all the possible combinations between all unique files
    # and all unique folders. This is used as an image of the expected folder structure
    def create_all_possible_combinations_files_folders(self):
        self.create_view_of_unique_files()
        self.create_view_of_unique_folders()
        self._current_cursor.execute(
            "CREATE VIEW " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + " AS SELECT fld." + self.absolute_path +
            " , file." + self.filename + " , file." + self.hash_tag +
            " FROM " + UNIQUE_FOLDERS_VIEW_NAME + " AS fld CROSS JOIN " +
            UNIQUE_FILES_VIEW_NAME + " AS file")
        self._db_connection.commit()

    ##################################################################################################
    # This function compares the complete (expected) folder structure with the existing one and returns a table with
    # the following structure:
    # [folder; filename of missing file from the folder; hash of missing file from the folder]
    def get_missing_files_from_all_folders(self):
        self.create_all_possible_combinations_files_folders()
        self._current_cursor.execute(
            "SELECT DISTINCT " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + ".* FROM " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME
            + " WHERE NOT EXISTS (SELECT * FROM " + self._all_files_table_name +
            " WHERE " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + "." + self.hash_tag + " = " +
            self._all_files_table_name + "." + self.hash_tag + " AND " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME
            + "." + self.absolute_path + " = " + self._all_files_table_name + "." +
            self.absolute_path + " );")
        return self._current_cursor.fetchall()

    ##################################################################################################

    def select_unique_files_by_hash(self):
        self._current_cursor.execute("SELECT DISTINCT " + self.filename + " , " + self.hash_tag +
                                     " FROM " + self._all_files_table_name)
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
