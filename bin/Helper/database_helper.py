import sqlite3
from enum import Enum
from .file_type import FileType

##################################################################################################

UNIQUE_FILES_VIEW_NAME = "unique_files"
UNIQUE_FOLDERS_VIEW_NAME = "unique_folders"
EXPECTED_FOLDER_STRUCTURE_VIEW_NAME = "expected_folder_structure"


##################################################################################################


def convert_file_type_to_sql_entry(file: FileType):
    """
    Helper function that converts a FileType object to the SQL row entry
    :param file: FileType object
    :return:
    """
    return "('{}','{}','{}','{}','{}','{}','{}','{}','{}')" \
        .format(file.filename,
                file.file_extension,
                file.file_size_kb,
                file.file_hash_tag,
                file.absolute_path,
                file.relative_path,
                file.absolute_path_hash_tag,
                file.last_modified_time,
                file.creation_time)


##################################################################################################

class SecretIndexTableColumnNames(Enum):
    """
    Enum that contains all the column names in the secret index table
    """
    absolute_path = "absolute_path"  # type: str
    relative_path = "relative_path"  # type: str
    filename = "filename"  # type: str
    file_extension = "file_extension"  # type: str
    file_hash_tag = "file_hash_tag"  # type: str
    absolute_path_hash_tag = "abs_path_hash_tag"  # type: str
    file_size_kb = "file_size_kb"  # type: str
    last_modification_time = "last_modification_time"  # type: str
    creation_time = "creation_time"  # type: str


class PublicIndexTableColumnNames(Enum):
    file_hash_tag = "file_hash_tag"  # type: str
    file_extension = "file_extension"  # type: str
    file_size_kb = "file_size_kb"  # type: str
    last_modification_time = "last_modification_time"  # type: str
    creation_time = "creation_time"  # type: str
    absolute_path_hash_tag = "abs_path_hash_tag"  # type: str


##################################################################################################


class DataBaseHelper:
    """
    Helper class that manages the database related functionality
    """

    def __init__(self, secret_db_path, public_db_path,
                 index_table_name="index_table",
                 result_table_name="result_table"):

        if secret_db_path is None or public_db_path is None:
            # We should never reach this point.
            raise ValueError("Database path is empty.")

        self._secret_index_table_name = "secret_" + index_table_name  # type:str
        self._secret_result_table_name = "secret_" + result_table_name  # type: str

        self._public_index_table_name = "public_" + index_table_name  # type:str
        self._public_result_table_name = "public_" + result_table_name  # type: str

        self._secret_database_path = secret_db_path  # type: str
        self._public_database_path = public_db_path  # type: str

        self._public_db_connection = sqlite3.connect(self._public_database_path)
        self._secret_db_connection = sqlite3.connect(self._secret_database_path)

        self._secret_db_cursor = self._secret_db_connection.cursor()
        self._public_db_cursor = self._public_db_connection.cursor()
        self._secret_db_cursor.execute("ATTACH DATABASE \"" + self._public_database_path + "\" AS public")
        self._secret_db_connection.commit()

        self.drop_all_tables_and_views()

        self.create_public_index_table()
        self.create_secret_index_table()

    ##################################################################################################

    def create_secret_index_table(self):
        """
        Helper function that creates the secret index table
        :return:
        """
        self._secret_db_cursor = self._secret_db_connection.cursor()
        self._secret_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            self._secret_index_table_name + " ( " +
            SecretIndexTableColumnNames.filename.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.file_extension.value + " text, " +
            SecretIndexTableColumnNames.file_size_kb.value + " real NOT NULL, " +
            SecretIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.absolute_path.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.relative_path.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            SecretIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            SecretIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            SecretIndexTableColumnNames.file_hash_tag.value +
            " ));")
        self._secret_db_connection.commit()

    ##################################################################################################

    def create_public_index_table(self):
        """
        Helper function that creates the public index table
        :return:
        """
        self._public_db_cursor = self._public_db_connection.cursor()
        self._public_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            self._public_index_table_name + " ( " +
            PublicIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.file_extension.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.file_size_kb.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            PublicIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            PublicIndexTableColumnNames.file_hash_tag.value +
            " ));")
        self._public_db_connection.commit()

    ##################################################################################################

    def _create_public_result_table(self):
        """
                Helper function that creates the public index table
                :return:
                """
        self._public_db_cursor = self._public_db_connection.cursor()
        self._public_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            self._public_result_table_name + " ( " +
            PublicIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.file_extension.value + " text NOT NULL, " +
            PublicIndexTableColumnNames.file_size_kb.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            PublicIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            PublicIndexTableColumnNames.file_hash_tag.value +
            " ));")
        self._public_db_connection.commit()

    ##################################################################################################

    def _insert_file_in_secret_table(self, file: FileType):
        """
        Helper function that inserts a single file in the database
        :param file: FileType object
        :return:
        """
        if file is None:
            return
        print(convert_file_type_to_sql_entry(file))
        self._secret_db_cursor.execute("INSERT INTO " + self._secret_index_table_name + " VALUES {}"
                                       .format(convert_file_type_to_sql_entry(file=file)))
        self._secret_db_connection.commit()

    ##################################################################################################

    def insert_files_in_both_databases(self, files: [FileType]):
        """
        Helper function that accepts a list of file objects to be inserted in the database with a single insert command.
        :param files: List of FileType objects
        :return:
        """
        if len(files) == 0:
            return
        for file in files:
            self._insert_file_in_secret_table(file)

        self._secret_db_connection.execute(
            "INSERT INTO public." + self._public_index_table_name
            + " SELECT " +
            SecretIndexTableColumnNames.file_hash_tag.value + " , " +
            SecretIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            SecretIndexTableColumnNames.file_extension.value + " , " +
            SecretIndexTableColumnNames.file_size_kb.value + " , " +
            SecretIndexTableColumnNames.last_modification_time.value + " , " +
            SecretIndexTableColumnNames.creation_time.value +
            " FROM " + self._secret_index_table_name)

    ##################################################################################################

    def get_all_rows_from(self, table_or_view: str = None):
        """
        Helper function that returns all the contents from the given table/view in the secret database.
        :param table_or_view: name of the table/view to select from
        :return:
        """
        if table_or_view is None:
            table_or_view = self._secret_index_table_name
        self._secret_db_cursor.execute("SELECT * FROM " + table_or_view)
        return self._secret_db_cursor.fetchall()

    ##################################################################################################

    def get_row_count_from_table_or_view(self, table_or_view: str = None):
        """
        Helper function that returns the number of rows in the given table/view.
        :param table_or_view: name of the table/view to select from
        :return:
        """
        if table_or_view is None:
            table_or_view = self._secret_index_table_name
        self._secret_db_cursor.execute("SELECT COUNT(*) FROM " + table_or_view)
        return self._secret_db_cursor.fetchone()

    ##################################################################################################

    def drop_all_tables_and_views(self):
        """
        Helper function that drops every table/view in the secret database
        :return:
        """
        self._secret_db_cursor.execute("DROP TABLE IF EXISTS " + self._secret_index_table_name)
        self._secret_db_cursor.execute("DROP TABLE IF EXISTS " + self._secret_result_table_name)
        self._secret_db_cursor.execute("DROP VIEW IF EXISTS " + UNIQUE_FILES_VIEW_NAME)
        self._secret_db_cursor.execute("DROP VIEW IF EXISTS " + UNIQUE_FOLDERS_VIEW_NAME)
        self._secret_db_cursor.execute("DROP VIEW IF EXISTS " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME)
        self._secret_db_connection.commit()

        self._public_db_cursor.execute("DROP TABLE IF EXISTS " + self._public_index_table_name)
        self._public_db_cursor.execute("DROP TABLE IF EXISTS " + self._public_result_table_name)
        self._public_db_connection.commit()

    ##################################################################################################

    def _create_view_of_unique_files(self):
        """
        Helper function that creates a view in the secret database that contains all the unique files found in the
        index table
        :return:
        """
        self._secret_db_cursor.execute("CREATE VIEW " + UNIQUE_FILES_VIEW_NAME + " AS " + "SELECT DISTINCT " +
                                       SecretIndexTableColumnNames.file_hash_tag.value + " , " +
                                       SecretIndexTableColumnNames.filename.value + " , " +
                                       SecretIndexTableColumnNames.file_extension.value + " , " +
                                       SecretIndexTableColumnNames.file_size_kb.value +
                                             " FROM " + self._secret_index_table_name)
        self._secret_db_connection.commit()

    ##################################################################################################

    def _create_view_of_unique_folders(self):
        """
        Helper function that creates a view in the secret database that contains all the unique folders found in the
        index table
        :return:
        """
        self._secret_db_cursor.execute("CREATE VIEW " + UNIQUE_FOLDERS_VIEW_NAME + " AS " + "SELECT DISTINCT " +
                                       SecretIndexTableColumnNames.absolute_path.value + " , " +
                                       SecretIndexTableColumnNames.absolute_path_hash_tag.value +
                                             " FROM " +
                                       self._secret_index_table_name)
        self._secret_db_connection.commit()

    ##################################################################################################

    def _create_all_possible_combinations_files_folders(self):
        """
        This function creates a table with all the possible combinations between all unique files and all unique
        folders. This is used as an image of the expected folder structure
        :return:
        """
        self._create_view_of_unique_files()
        self._create_view_of_unique_folders()
        self._secret_db_cursor.execute(
            "CREATE VIEW " + EXPECTED_FOLDER_STRUCTURE_VIEW_NAME +
            " AS SELECT " +
            " file." + SecretIndexTableColumnNames.filename.value +
            " , file." + SecretIndexTableColumnNames.file_extension.value +
            " , file." + SecretIndexTableColumnNames.file_size_kb.value +
            " , file." + SecretIndexTableColumnNames.file_hash_tag.value +
            " , folder." + SecretIndexTableColumnNames.absolute_path.value +
            " , folder." + SecretIndexTableColumnNames.absolute_path_hash_tag.value +
            " FROM " + UNIQUE_FOLDERS_VIEW_NAME + " AS folder CROSS JOIN " +
            UNIQUE_FILES_VIEW_NAME + " AS file")
        self._secret_db_connection.commit()

    ##################################################################################################

    def create_secret_result_table(self):
        """
        This function compares the complete (expected) folder structure with the existing one and creates a table with
        the following columns:
        [folder absolute path; filename of missing file from the folder; hash of missing file from the folder]
        :return: None
        """

        self._create_all_possible_combinations_files_folders()
        self._secret_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " + self._secret_result_table_name +
            " AS SELECT DISTINCT " +
            EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + ".*" + " FROM " +

            EXPECTED_FOLDER_STRUCTURE_VIEW_NAME +

            " WHERE NOT EXISTS (SELECT * FROM " +
            self._secret_index_table_name +
            " WHERE " +
            EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + "." + SecretIndexTableColumnNames.file_hash_tag.value + " = " +
            self._secret_index_table_name + "." + SecretIndexTableColumnNames.file_hash_tag.value +

            " AND " +

            EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + "." + SecretIndexTableColumnNames.absolute_path.value + " = " +
            self._secret_index_table_name + "." + SecretIndexTableColumnNames.absolute_path.value +

            " );")
        self._secret_db_connection.commit()

        self._create_public_result_table()

        self._secret_db_connection.execute(
            "INSERT INTO public." + self._public_result_table_name
            + " SELECT " +
            SecretIndexTableColumnNames.file_hash_tag.value + " , " +
            SecretIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            SecretIndexTableColumnNames.file_extension.value + " , " +
            SecretIndexTableColumnNames.file_size_kb.value +
            " FROM " + self._secret_result_table_name)

        self._secret_db_connection.commit()
        #
        # self._public_db_cursor.execute("ALTER TABLE " + self._public_result_table_name +
        #                                " ADD COLUMN " + PublicIndexTableColumnNames.last_modification_time.value +
        #                                " text;")
        #
        # self._public_db_cursor.execute("ALTER TABLE " + self._public_result_table_name +
        #                                " ADD COLUMN " + PublicIndexTableColumnNames.creation_time.value +
        #                                " text")
        #
        # self._public_db_cursor.execute(
        #     "UPDATE " + self._public_result_table_name +
        #
        #     " SET " +
        #
        #     self._public_result_table_name + "." + PublicIndexTableColumnNames.last_modification_time.value + " = " +
        #     self._public_index_table_name + "." + PublicIndexTableColumnNames.last_modification_time.value +
        #
        #     " , " +
        #
        #     self._public_result_table_name + "." + PublicIndexTableColumnNames.creation_time.value + " = " +
        #     self._public_index_table_name + "." + PublicIndexTableColumnNames.creation_time.value +
        #
        #     " WHERE " +
        #
        #     self._public_index_table_name + "." + PublicIndexTableColumnNames.file_hash_tag.value +
        #     " = " +
        #     self._public_result_table_name + "." + PublicIndexTableColumnNames.file_hash_tag.value
        # )
        #
        # self._public_db_connection.commit()

    ##################################################################################################
