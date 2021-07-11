import sqlite3
from datetime import timedelta
from timeit import default_timer as timer

from backports.strenum import StrEnum  # sudo pip install backports.strenum

from .config_file_handler import EvaluationConfiguration
from .databases_config_mixin import DatabasesConfigMixin
from .evaluation_config_mixin import EvaluationConfigMixin
from .file_type import FileType


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

class DataBase(object):
    ##################################################################################################

    class PrivateIndexTableColumnNames(StrEnum):
        """
        Enum that contains all the column names in the private index table
        """
        absolute_path = "absolute_path"
        relative_path = "relative_path"
        filename = "filename"
        file_extension = "file_extension"
        file_hash_tag = "file_hash_tag"
        absolute_path_hash_tag = "abs_path_hash_tag"
        file_size_kb = "file_size_kb"
        last_modification_time = "last_modification_time"
        creation_time = "creation_time"

    ##################################################################################################

    class PublicIndexTableColumnNames(StrEnum):
        file_hash_tag = "file_hash_tag"
        file_extension = "file_extension"
        file_size_kb = "file_size_kb"
        last_modification_time = "last_modification_time"
        creation_time = "creation_time"
        absolute_path_hash_tag = "abs_path_hash_tag"

    ##################################################################################################

    def __init__(self, database_config: DatabasesConfigMixin, index_table_name: str = "index_table"):
        private_db_path = database_config.get_private_database_path()
        public_db_path = database_config.get_public_database_path()

        if private_db_path is None or public_db_path is None:
            # We should never reach this point.
            raise ValueError("Database path is empty.")

        self.private_index_table_name = "inp_private_" + index_table_name  # type:str

        self.public_index_table_name = "inp_public_" + index_table_name  # type:str

        self._private_database_path = private_db_path  # type: str
        self._public_database_path = public_db_path  # type: str

        self._public_db_connection = sqlite3.connect(self._public_database_path)
        self._private_db_connection = sqlite3.connect(self._private_database_path)

        self.private_db_cursor = self._private_db_connection.cursor()
        self.public_db_cursor = self._public_db_connection.cursor()
        self.private_db_cursor.execute("ATTACH DATABASE \"" + self._public_database_path + "\" AS public")

        self.drop_all_tables_and_views()

        self.create_public_index_table()
        self.create_private_index_table()

    ##################################################################################################

    def create_private_index_table(self):
        """
        Helper function that creates the private index table
        :return:
        """
        self.private_db_cursor = self._private_db_connection.cursor()
        self.private_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            self.private_index_table_name + " ( " +
            DataBase.PrivateIndexTableColumnNames.filename.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.file_extension.value + " text, " +
            DataBase.PrivateIndexTableColumnNames.file_size_kb.value + " real NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.absolute_path.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.relative_path.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            DataBase.PrivateIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            DataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            DataBase.PrivateIndexTableColumnNames.file_hash_tag.value +
            " ));")

    ##################################################################################################

    def create_public_index_table(self):
        """
        Helper function that creates the public index table
        :return:
        """
        self.public_db_cursor = self._public_db_connection.cursor()
        self.public_db_cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            self.public_index_table_name + " ( " +
            DataBase.PublicIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            DataBase.PublicIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            DataBase.PublicIndexTableColumnNames.file_extension.value + " text NOT NULL, " +
            DataBase.PublicIndexTableColumnNames.file_size_kb.value + " text NOT NULL, " +
            DataBase.PublicIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            DataBase.PublicIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            DataBase.PublicIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            DataBase.PublicIndexTableColumnNames.file_hash_tag.value +
            " ));")

    ##################################################################################################

    def close_connections(self):
        self._public_db_connection.commit()
        self._private_db_connection.commit()

        self._public_db_connection.close()
        self._private_db_connection.close()

    ##################################################################################################

    def drop_all_tables_and_views(self):
        """
        Helper function that drops every table/view in the database
        :return:
        """
        self.private_db_cursor.execute("DROP TABLE IF EXISTS " + self.private_index_table_name)
        self.public_db_cursor.execute("DROP TABLE IF EXISTS " + self.public_index_table_name)


##################################################################################################


class DataBaseIndexHelper(DataBase):
    """
    Helper class for writing file information to tables.
    """

    def __init__(self, database_config: DatabasesConfigMixin, index_table_name="index_table"):
        DataBase.__init__(self, database_config, index_table_name=index_table_name)

    ##################################################################################################

    def insert_files_in_both_databases(self, files: [FileType]):
        """
        Helper function that accepts a list of file objects to be inserted in the database with a single insert command.
        :param files: List of FileType objects
        :return:
        """
        print("Storing data sets to private database ...")
        if len(files) <= 0:
            return

        for file in files:
            self._insert_file_in_private_table(file)

        print("Storing data sets to public database ...")

        self._private_db_connection.execute(
            "INSERT INTO public." + self.public_index_table_name
            + " SELECT " +
            DataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " , " +
            DataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            DataBase.PrivateIndexTableColumnNames.file_extension.value + " , " +
            DataBase.PrivateIndexTableColumnNames.file_size_kb.value + " , " +
            DataBase.PrivateIndexTableColumnNames.last_modification_time.value + " , " +
            DataBase.PrivateIndexTableColumnNames.creation_time.value +
            " FROM " + self.private_index_table_name)

        print("Storing data to database done.")

    ##################################################################################################

    def _insert_file_in_private_table(self, file: FileType):
        """
        Helper function that inserts a single file in the database
        :param file: FileType object
        :return:
        """
        if file is None:
            return

        try:
            self.private_db_cursor.execute("INSERT INTO " + self.private_index_table_name + " VALUES {}"
                                           .format(convert_file_type_to_sql_entry(file)))
        except sqlite3.Error as e:
            print(convert_file_type_to_sql_entry(file))
            raise e

    ##################################################################################################

    '''
    def get_all_rows_from(self, table_or_view: str = None):
        """
        Helper function that returns all the contents from the given table/view in the private database.
        :param table_or_view: name of the table/view to select from
        :return:
        """
        if table_or_view is None:
            table_or_view = self._private_index_table_name
        self._private_db_cursor.execute("SELECT * FROM " + table_or_view)
        return self._private_db_cursor.fetchall()
    '''

    ##################################################################################################

    '''
    def get_row_count_from_table_or_view(self, table_or_view: str = None):
        """
        Helper function that returns the number of rows in the given table/view.
        :param table_or_view: name of the table/view to select from
        :return:
        """
        if table_or_view is None:
            table_or_view = self._private_index_table_name
        self._private_db_cursor.execute("SELECT COUNT(*) FROM " + table_or_view)
        return self._private_db_cursor.fetchone()
    '''


######################################################################################################

class DataBaseEvaluationHelper(DataBase):
    ##################################################################################################
    """
    Helper class that helps evaluating the file information.
    """

    def __init__(self, database_config: DatabasesConfigMixin, evaluation_config: EvaluationConfigMixin):
        DataBase.__init__(self, database_config)

        evaluation_db_path = evaluation_config.get_evaluation_database_path()
        if evaluation_db_path is None:
            raise ValueError("Database path is empty.")

        self._evaluation_database_path = evaluation_db_path  # type: str
        self._evaluation_db_connection = sqlite3.connect(self._evaluation_database_path)
        self.evaluation_db_cursor = self._evaluation_db_connection.cursor()

        self.public_db_cursor.execute("ATTACH DATABASE \"" + self._evaluation_database_path + "\" AS evaluation")

    def close_connections(self):
        self._evaluation_db_connection.commit()
        self._evaluation_db_connection.close()
        DataBase.close_connections(self)


######################################################################################################

class UniqueFileFolderEvaluator(object):
    """
    Evaluator that computes the unique files and unique folders.
    """
    ##################################################################################################

    UNIQUE_FILES_VIEW_NAME = "unique_files"
    UNIQUE_FOLDERS_VIEW_NAME = "unique_folders"

    ##################################################################################################

    def __init__(self, database_helper: DataBaseEvaluationHelper, evaluation_table_name: str = "unique_entities"):
        self._data_base = database_helper
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        print("\n[UniqueFileFolderEvaluator START]")
        start_timestamp = timer()
        self.drop_all_tables_and_views()
        self._create_view_of_unique_files()
        self._create_view_of_unique_folders()
        # TODO - implement: create a real table instead of view (because view is not in same db as data table)
        assert False
        print("[UniqueFileFolderEvaluator END] Time elapsed {}".format(timedelta(seconds=timer() - start_timestamp)))

    ##################################################################################################

    def drop_all_tables_and_views(self):
        self._data_base.evaluation_db_cursor.execute(
            "DROP VIEW IF EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FILES_VIEW_NAME)
        self._data_base.evaluation_db_cursor.execute(
            "DROP VIEW IF EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_VIEW_NAME)

    ##################################################################################################

    def _create_view_of_unique_files(self):
        """
        Helper function that creates a view in the private database that contains all the unique files found in the
        index table
        :return:
        """
        self._data_base.evaluation_db_cursor.execute(
            "CREATE VIEW " + UniqueFileFolderEvaluator.UNIQUE_FILES_VIEW_NAME + " AS " +
            "SELECT DISTINCT " +
            DataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " , " +
            DataBase.PrivateIndexTableColumnNames.filename.value + " , " +
            DataBase.PrivateIndexTableColumnNames.file_extension.value + " , " +
            DataBase.PrivateIndexTableColumnNames.file_size_kb.value +
            " FROM " + self._data_base.private_index_table_name)

    ##################################################################################################

    def _create_view_of_unique_folders(self):
        """
        Helper function that creates a view in the private database that contains all the unique folders found in the
        index table
        :return:
        """
        self._data_base.evaluation_db_cursor.execute(
            "CREATE VIEW " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_VIEW_NAME + " AS " + "SELECT DISTINCT " +
            DataBase.PrivateIndexTableColumnNames.absolute_path.value + " , " +
            DataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value +
            " FROM " +
            self._data_base.private_index_table_name)


######################################################################################################

class ExpectedFolderStructureEvaluator(object):
    """
    Evaluator that constructs the union of all folders (expected folder structure).
    The evaluator depends on the result of UniqueFileFolderEvaluator.evaluate().
    """
    ##################################################################################################

    UNIQUE_FILES_VIEW_NAME = "unique_files"
    UNIQUE_FOLDERS_VIEW_NAME = "unique_folders"
    EXPECTED_FOLDER_STRUCTURE_VIEW_NAME = "expected_folder_structure"

    ##################################################################################################

    def __init__(self, database_helper: DataBaseEvaluationHelper,
                 evaluation_table_name: str = "expected_folder_structure"):
        self._data_base = database_helper
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        print("\n[ExpectedFolderStructureEvaluator START]")
        start_timestamp = timer()
        self.drop_all_tables_and_views()
        # TODO - implement: create a real table instead of view (because view is not in same db as data table)
        assert False
        self._create_view_of_expected_folder_structure()
        print("[ExpectedFolderStructureEvaluator END] Time elapsed {}"
              .format(timedelta(seconds=timer() - start_timestamp)))

    ##################################################################################################

    def drop_all_tables_and_views(self):
        """
        Helper function that drops every table/view in the database
        :return:
        """
        self._data_base.evaluation_db_cursor.execute(
            "DROP VIEW IF EXISTS " + ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME)

        self._data_base.drop_all_tables_and_views()

    ##################################################################################################

    def _create_view_of_expected_folder_structure(self):
        self._data_base.evaluation_db_cursor.execute(
            "CREATE VIEW " + ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME +
            " AS SELECT " +
            " file." + DataBase.PrivateIndexTableColumnNames.filename.value +
            " , file." + DataBase.PrivateIndexTableColumnNames.file_extension.value +
            " , file." + DataBase.PrivateIndexTableColumnNames.file_size_kb.value +
            " , file." + DataBase.PrivateIndexTableColumnNames.file_hash_tag.value +
            " , folder." + DataBase.PrivateIndexTableColumnNames.absolute_path.value +
            " , folder." + DataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value +
            " FROM " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_VIEW_NAME + " AS folder CROSS JOIN " +
            UniqueFileFolderEvaluator.UNIQUE_FILES_VIEW_NAME + " AS file")


######################################################################################################

class TODOEvaluator(object):
    ##################################################################################################

    EXPECTED_FOLDER_STRUCTURE_VIEW_NAME = "expected_folder_structure"

    ##################################################################################################
    """
    Helper class that helps evaluating the file information.
    """

    def __init__(self, database_helper: DataBaseEvaluationHelper,
                 evaluation_table_name: str = "expected_folder_structure"):
        self.drop_all_tables_and_views()

        self._data_base = database_helper
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        self.drop_all_tables_and_views()
        self._data_base.drop_all_tables_and_views()

    ##################################################################################################

    def drop_all_tables_and_views(self):
        pass
        # self._data_base.private_db_cursor.execute(
        #    "DROP VIEW IF EXISTS " + UniqueFileFolderEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME)

    ##################################################################################################

    def create_private_result_table(self):
        """
        This function compares the complete (expected) folder structure with the existing one and creates a table with
        the following columns:
        [folder absolute path; filename of missing file from the folder; hash of missing file from the folder]
        :return: None
        """

        # self._create_view_of_all_possible_combinations_files_folders()

        # print("Create result table ...")
        # sys.stdout.flush()
        # self._data_base.private_db_cursor.execute(
        #    "CREATE TABLE IF NOT EXISTS " + self._data_base.private_result_table_name +
        #    " AS SELECT DISTINCT " +
        #    TODOEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + ".*" + " FROM " +

        #    TODOEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME +

        #    " WHERE NOT EXISTS (SELECT * FROM " +
        #    self._data_base.private_index_table_name +
        #    " WHERE " +
        #    TODOEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + "." +
        #    DataBase.privateIndexTableColumnNames.file_hash_tag.value + " = " +
        #    self._data_base.private_index_table_name + "." +
        #    DataBase.privateIndexTableColumnNames.file_hash_tag.value +

        #    " AND " +

        #    TODOEvaluator.EXPECTED_FOLDER_STRUCTURE_VIEW_NAME + "." +
        #    DataBase.privateIndexTableColumnNames.absolute_path.value + " = " +
        #    self._data_base.private_index_table_name + "." +
        #    DataBase.privateIndexTableColumnNames.absolute_path.value +

        #    " );")

        # --
        #
        # self.public_db_cursor.execute("ALTER TABLE " + self._public_result_table_name +
        #                                " ADD COLUMN " + PublicIndexTableColumnNames.last_modification_time.value +
        #                                " text;")
        #
        # self.public_db_cursor.execute("ALTER TABLE " + self._public_result_table_name +
        #                                " ADD COLUMN " + PublicIndexTableColumnNames.creation_time.value +
        #                                " text")
        #
        # self.public_db_cursor.execute(
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
