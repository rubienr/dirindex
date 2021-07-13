import sqlite3
from abc import abstractmethod
from datetime import timedelta
from timeit import default_timer as timer

from backports.strenum import StrEnum  # sudo pip install backports.strenum

from .databases_config_mixin import DatabaseConfigMixin
from .evaluation_config_mixin import EvaluationConfigMixin
from .file_type import FileType


######################################################################################################


def convert_file_type_to_sql_entry(file: FileType):
    """
    Helper function that converts a FileType object to the SQL row entry
    :param file: FileType object
    :return:
    """
    return "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')" \
        .format(file.filename,
                file.file_extension,
                file.file_size_kb,
                file.file_hash_tag,
                file.absolute_path,
                file.relative_path,
                file.absolute_path_hash_tag,
                file.relative_path_hash_tag,
                file.last_modified_time,
                file.creation_time)


######################################################################################################

class SqliteDbConnector(object):
    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin):
        db_path = database_config.get_database_path()

        if db_path is None:
            raise ValueError("Database path is empty.")

        self.__database_path = db_path  # type: str
        self.__db_connection = sqlite3.connect(self.__database_path)  # type: sqlite3.Connection
        self.__db_cursor = self.__db_connection.cursor()  # type: sqlite3.Cursor

    ##################################################################################################

    def cursor(self): return self.__db_cursor

    ##################################################################################################

    def connection(self): return self.__db_connection

    ##################################################################################################

    def database_path(self): return self.__database_path

    ##################################################################################################

    def close(self):
        self.__db_connection.commit()
        self.__db_connection.close()


######################################################################################################

class IndexDataBaseHelper(SqliteDbConnector):
    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, table_name: str):
        super().__init__(database_config)
        db_path = database_config.get_database_path()

        if db_path is None:
            raise ValueError("Database path is empty.")

        self.__table_name = "inp_" + table_name  # type:str
        self.__database_path = db_path  # type: str
        self.__db_connection = sqlite3.connect(self.__database_path)
        self.__db_cursor = self.__db_connection.cursor()

    ##################################################################################################

    def table_name(self): return self.__table_name

    ##################################################################################################

    def drop_all_tables_and_views(self):
        self.__db_cursor.execute("DROP TABLE IF EXISTS " + self.table_name())


##################################################################################################

class PrivateDataBase(IndexDataBaseHelper):
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
        relative_path_hash_tag = "rel_path_hash_tag"
        file_size_kb = "file_size_kb"
        last_modification_time = "last_modification_time"
        creation_time = "creation_time"

    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, private_index_table_name: str = "priv_index_table"):
        super().__init__(database_config, table_name=private_index_table_name)

    ##################################################################################################

    def reset(self):
        self.drop_all_tables_and_views()
        self._create_index_table()

    ##################################################################################################

    def _create_index_table(self):
        """
        Helper function that creates the private index table
        :return:
        """
        self.cursor().execute(
            "CREATE TABLE IF NOT EXISTS " +
            self.table_name() + " ( " +
            PrivateDataBase.PrivateIndexTableColumnNames.filename.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.file_extension.value + " text, " +
            PrivateDataBase.PrivateIndexTableColumnNames.file_size_kb.value + " real NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.absolute_path.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.relative_path.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            PrivateDataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value +
            " ));")


##################################################################################################

class PublicDataBase(IndexDataBaseHelper):
    ##################################################################################################

    class PublicIndexTableColumnNames(StrEnum):
        file_hash_tag = "file_hash_tag"
        file_extension = "file_extension"
        file_size_kb = "file_size_kb"
        last_modification_time = "last_modification_time"
        creation_time = "creation_time"
        absolute_path_hash_tag = "abs_path_hash_tag"
        relative_path_hash_tag = "rel_path_hash_tag"

    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, public_index_table_name: str = "pub_index_table"):
        super().__init__(database_config, table_name=public_index_table_name)

    ##################################################################################################

    def reset(self):
        self.drop_all_tables_and_views()
        self._create_index_table()

    ##################################################################################################

    def _create_index_table(self):
        """
        Helper function that creates the public index table
        :return:
        """
        self.cursor().execute(
            "CREATE TABLE IF NOT EXISTS " +
            self.table_name() + " ( " +
            PublicDataBase.PublicIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.absolute_path_hash_tag.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.file_extension.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.file_size_kb.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.last_modification_time.value + " text NOT NULL, " +
            PublicDataBase.PublicIndexTableColumnNames.creation_time.value + " text NOT NULL, " +

            "PRIMARY KEY ( " +
            PublicDataBase.PublicIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
            PublicDataBase.PublicIndexTableColumnNames.file_hash_tag.value +
            " ));")


##################################################################################################

class IndexDataBases(object):
    ##################################################################################################

    def __init__(self, private_db_config: DatabaseConfigMixin,
                 public_db_config: DatabaseConfigMixin, **kwargs):
        self.private_db = PrivateDataBase(private_db_config, **kwargs)
        self.public_db = PublicDataBase(public_db_config, **kwargs)
        self._dbs = [self.private_db, self.public_db]

        # Attach the public database to the private db. to allow db-spanning queries by use of the private db cursor.
        self.private_db.cursor().execute("ATTACH DATABASE \"" + self.public_db.database_path() + "\" AS public")

    ##################################################################################################

    def close(self):  [db.close() for db in self._dbs]

    ##################################################################################################

    def reset(self):  [db.reset() for db in self._dbs]

    ##################################################################################################

    def drop_all_tables_and_views(self):  [db.drop_all_tables_and_views() for db in self._dbs]


##################################################################################################

class DataBaseIndexHelper(IndexDataBases):

    ##################################################################################################

    def __init__(self, private_db_config: DatabaseConfigMixin, public_db_config: DatabaseConfigMixin, **kwargs):
        super().__init__(private_db_config, public_db_config, **kwargs)
        self.reset()

    ##################################################################################################

    def insert_files_in_both_databases(self, files: [FileType]):
        """
        Helper function that accepts a list of file objects to be inserted in the database with a single insert command.
        :param files: List of FileType objects
        :return:
        """
        print("Storing data sets to private database ...")
        if len(files) > 0:
            for file in files:
                self._insert_file_in_private_table(file)

            print("Storing data sets to public database ...")

            self.private_db.connection().execute(
                "INSERT INTO public." + self.public_db.table_name()
                + " SELECT " +
                PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.absolute_path_hash_tag.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.file_extension.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.file_size_kb.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.last_modification_time.value + " , " +
                PrivateDataBase.PrivateIndexTableColumnNames.creation_time.value +
                " FROM " + self.private_db.table_name())

        print("Storing data to database done.")

    ##################################################################################################

    def _insert_file_in_private_table(self, file: FileType):
        if file is None:
            return

        try:
            self.private_db.cursor().execute(
                "INSERT INTO " + self.private_db.table_name() +
                " VALUES {}".format(convert_file_type_to_sql_entry(file)))
        except sqlite3.Error as e:
            print(convert_file_type_to_sql_entry(file))
            raise e
        except object:
            assert False

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

class EvaluationDataBases(object):
    ##################################################################################################

    def __init__(self, public_index_db_config: DatabaseConfigMixin, evaluation_db_config: DatabaseConfigMixin):
        self.index_db = PublicDataBase(public_index_db_config)
        self.evaluation_db = SqliteDbConnector(evaluation_db_config)
        self._dbs = [self.index_db, self.evaluation_db]

        # Attach the public database to the private db. to allow db-spanning queries by use of the private db cursor.
        self.evaluation_db.cursor().execute(
            "ATTACH DATABASE \"" + self.index_db.database_path() + "\" AS index_db")

    ##################################################################################################

    def close(self):  [db.close() for db in self._dbs]


######################################################################################################

class UniqueFileFolderEvaluator(object):
    """
    Evaluator that computes the unique files and unique folders.
    """
    ##################################################################################################

    UNIQUE_FILES_TABLE_NAME = "unique_files"
    UNIQUE_FOLDERS_TABLE_NAME = "unique_folders"

    ##################################################################################################

    def __init__(self, databases: EvaluationDataBases, evaluation_table_name: str = "unique_entities"):
        self.index_db = databases.index_db
        self.evaluation_db = databases.evaluation_db
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        print("\n[UniqueFileFolderEvaluator START]")
        start_timestamp = timer()

        self.reset()
        self._insert_into_table_of_unique_files()
        self._insert_into_table_of_unique_folders()

        print("[UniqueFileFolderEvaluator END] Time elapsed {}".format(timedelta(seconds=timer() - start_timestamp)))

    ##################################################################################################

    def reset(self):
        self._drop_all_tables_and_views()
        self._create_table_of_unique_files()
        self._create_table_of_unique_folders()

    ##################################################################################################

    def _drop_all_tables_and_views(self):
        self.evaluation_db.cursor().execute(
            "DROP TABLE IF EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME)
        self.evaluation_db.cursor().execute(
            "DROP TABLE IF EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME)

    ##################################################################################################

    def _create_table_of_unique_files(self):
        self.evaluation_db.cursor().execute(
            "CREATE TABLE IF NOT EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME + " ( " +
            PublicDataBase.PublicIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            "count INTEGER NOT NULL " +
            " );")


    ##################################################################################################

    def _insert_into_table_of_unique_files(self):
        self.evaluation_db.cursor().execute(
            "INSERT INTO " + UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME +
            " SELECT " + PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + "," +
            " COUNT(" + PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + ")" +
            " FROM index_db." + self.index_db.table_name() +
            " GROUP BY " + PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value)

    ##################################################################################################

    def _create_table_of_unique_folders(self):
        self.evaluation_db.cursor().execute(
            "CREATE TABLE IF NOT EXISTS " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME + " ( " +
            PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value + " text NOT NULL, " +
            "count INTEGER NOT NULL " +
            " );")

    ##################################################################################################

    def _insert_into_table_of_unique_folders(self):
        self.evaluation_db.cursor().execute(
            "INSERT INTO " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME +
            " SELECT " + PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value + "," +
            " COUNT(" + PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value + ") " +
            " FROM index_db." + self.index_db.table_name() +
            " GROUP BY " + PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value)


######################################################################################################

class ExpectedFolderStructureEvaluator(object):
    """
    Evaluator that constructs the union of all folders (expected folder structure).
    The evaluator depends on the result of UniqueFileFolderEvaluator.evaluate().
    """
    ##################################################################################################

    EXPECTED_FOLDER_STRUCTURE_TABLE_NAME = "expected_folder_structure"

    ##################################################################################################

    def __init__(self, databases: EvaluationDataBases, evaluation_table_name: str = "expected_folder_structure"):
        self.index_db = databases.index_db
        self.evaluation_db = databases.evaluation_db
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        print("\n[ExpectedFolderStructureEvaluator START]")
        start_timestamp = timer()

        self.reset()
        self._insert_into_table_of_expected_folder_structure()

        print("[ExpectedFolderStructureEvaluator END] Time elapsed {}"
              .format(timedelta(seconds=timer() - start_timestamp)))

    ##################################################################################################

    def reset(self):
        self.drop_all_tables_and_views()
        self._create_table_of_expected_folder_structure()

    ##################################################################################################

    def drop_all_tables_and_views(self):
        self.evaluation_db.cursor().execute(
            "DROP TABLE IF EXISTS " + ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_TABLE_NAME)

    ##################################################################################################

    def _create_table_of_expected_folder_structure(self):
        self.evaluation_db.cursor().execute(
            "CREATE TABLE IF NOT EXISTS " + ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_TABLE_NAME +
            " ( " +
            PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + " text NOT NULL, " +
            PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value + " text NOT NULL " +
            " );")

    ##################################################################################################

    def _insert_into_table_of_expected_folder_structure(self):
        # TODO - this query explodes the data too much. Suggestion:
        # TODO - 1. select distinct the file content hash as distinct_files
        # TODO - 2. left join the index table on distinct_files
        # TODO - 3. remove duplicates (duplicate relative folders)

        """
        select distinct C.rel_path_hash_tag, C.file_hash_tag, C.cnt from
        (select
            *
        from
            (select
                file_hash_tag,
                count(file_hash_tag) as cnt
            from
                inp_priv_index_table
            GROUP by file_hash_tag ) AS A
        left join
            (select rel_path_hash_tag, file_hash_tag from inp_priv_index_table ) as B on
            B.file_hash_tag = A.file_hash_tag) AS c
        """

        self.evaluation_db.cursor().execute(
            "INSERT INTO " + ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_TABLE_NAME +
            " SELECT " +
            "  file." + PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value + ", "
            "  folder." + PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value +
            " FROM " + UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME + " AS folder " +
            " CROSS JOIN " + UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME + " AS file")

######################################################################################################

class TODOEvaluator(object):
    ##################################################################################################

    EXPECTED_FOLDER_STRUCTURE_VIEW_NAME = "expected_folder_structure"

    ##################################################################################################
    """
    Helper class that helps evaluating the file information.
    """

    def __init__(self, databases: EvaluationDataBases, evaluation_table_name: str = "expected_folder_structure"):
        self.index_db = databases.index_db
        self.evaluation_db = databases.evaluation_db
        self._evaluation_table_name = "eval_" + evaluation_table_name  # type: str

    ##################################################################################################

    def evaluate(self):
        self.drop_all_tables_and_views()

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
