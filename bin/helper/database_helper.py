import sqlite3
from abc import abstractmethod
from datetime import timedelta
from timeit import default_timer as timer
from typing import List, Tuple

from backports.strenum import StrEnum  # sudo pip install backports.strenum

from .databases_config_mixin import DatabaseConfigMixin
from .file_type import FileType


######################################################################################################


def convert_file_type_to_sql_entry(file: FileType) -> str:
    """
    Helper function that converts a FileType object to the SQL row entry
    """
    return "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')" \
        .format(file.root_path,
                file.relative_path,
                file.filename,
                file.file_extension,
                file.file_mime_type,
                file.root_path_hash_tag,
                file.relative_path_hash_tag,
                file.filename_hash_tag,
                file.absolute_file_path_hash_tag,
                file.file_content_hash_tag,
                file.creation_time,
                file.last_modification_time,
                file.file_size)


######################################################################################################

def convert_file_type_list_to_tuple_list(files: List[FileType]) \
        -> List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, int]]:
    """
    Helper function that converts a List[FileType] to List[Tuple] for sqlite3.cursor.executemany().
    """
    return [(f.root_path,
             f.relative_path,
             f.filename,
             f.file_extension,
             f.file_mime_type,
             f.root_path_hash_tag,
             f.relative_path_hash_tag,
             f.filename_hash_tag,
             f.absolute_file_path_hash_tag,
             f.file_content_hash_tag,
             f.creation_time,
             f.last_modification_time,
             f.file_size) for f in files]


######################################################################################################

class SqliteDbConnector(object):
    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin):
        db_path = database_config.get_database_path()

        if db_path is None:
            raise ValueError("Database path is empty.")

        self._database_path = db_path  # type: str
        self._db_connection = sqlite3.connect(self._database_path)  # type: sqlite3.Connection
        self._db_cursor = self._db_connection.cursor()  # type: sqlite3.Cursor

    ##################################################################################################

    def cursor(self): return self._db_cursor

    ##################################################################################################

    def connection(self): return self._db_connection

    ##################################################################################################

    def database_path(self): return self._database_path

    ##################################################################################################

    def close(self):
        self._db_connection.commit()
        self._db_connection.close()


######################################################################################################

class IndexDataBaseHelper(SqliteDbConnector):
    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, table_name: str):
        super().__init__(database_config)
        db_path = database_config.get_database_path()

        if db_path is None:
            raise ValueError("Database path is empty.")

        self._table_name = "inp_" + table_name  # type:str

    ##################################################################################################

    def table_name(self): return self._table_name

    ##################################################################################################

    def drop_all_tables_and_views(self):
        self._db_cursor.execute("DROP TABLE IF EXISTS {}".format(self.table_name()))


##################################################################################################

class PrivateDataBase(IndexDataBaseHelper):
    ##################################################################################################

    class PrivateIndexTableColumnNames(StrEnum):
        """
        Enum containing all the column names in the private index table.
        """
        root_path = "absolute_path"
        relative_path = "relative_path"
        filename = "filename"
        file_extension = "file_extension"
        file_mime_type = "file_mime_type"
        root_path_hash_tag = "root_path_hash_tag"
        relative_path_hash_tag = "rel_path_hash_tag"
        filename_hash_tag = "filename_hash_tag"
        absolute_file_path_hash_tag = "absolute_file_path_hash_tag"
        file_content_hash_tag = "file_content_hash_tag"
        creation_time = "creation_time"
        last_modification_time = "last_modification_time"
        file_size = "file_size"

    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, private_index_table_name: str = "priv_index_table"):
        super().__init__(database_config, table_name=private_index_table_name)

    ##################################################################################################

    def reset(self):
        self.drop_all_tables_and_views()
        self._create_index_table()

    ##################################################################################################

    def _create_index_table(self):
        q = """
        CREATE TABLE IF NOT EXISTS {tbl} 
        (
            {proot} TEXT NOT NULL,
            {prel} TEXT NOT NULL,
            {fname} TEXT NOT NULL,
            {fext} TEXT,
            {fmime} TEXT,

            {prooth} TEXT NOT NULL,
            {prelh} TEXT NOT NULL,
            {fnameh} TEXT NOT NULL,
            {pabsfnameh} TEXT NOT NULL,
            {fconth} TEXT NOT NULL,

            {ctime} TEXT NOT NULL,
            {mtime} TEXT NOT NULL,
            {fsize} INTEGER NOT NULL,

            PRIMARY KEY
            (
                {prooth},
                {prelh},
                {fnameh}
            )
        )
        """.format(
            tbl=self.table_name(),

            proot=PrivateDataBase.PrivateIndexTableColumnNames.root_path.value,
            prel=PrivateDataBase.PrivateIndexTableColumnNames.relative_path.value,
            fname=PrivateDataBase.PrivateIndexTableColumnNames.filename.value,
            fext=PrivateDataBase.PrivateIndexTableColumnNames.file_extension.value,
            fmime=PrivateDataBase.PrivateIndexTableColumnNames.file_mime_type.value,

            prooth=PrivateDataBase.PrivateIndexTableColumnNames.root_path_hash_tag.value,
            prelh=PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value,
            fnameh=PrivateDataBase.PrivateIndexTableColumnNames.filename_hash_tag.value,
            pabsfnameh=PrivateDataBase.PrivateIndexTableColumnNames.absolute_file_path_hash_tag.value,
            fconth=PrivateDataBase.PrivateIndexTableColumnNames.file_content_hash_tag.value,

            ctime=PrivateDataBase.PrivateIndexTableColumnNames.creation_time.value,
            mtime=PrivateDataBase.PrivateIndexTableColumnNames.last_modification_time.value,
            fsize=PrivateDataBase.PrivateIndexTableColumnNames.file_size.value
        )
        self.cursor().execute(q)


##################################################################################################

class PublicDataBase(IndexDataBaseHelper):
    ##################################################################################################

    class PublicIndexTableColumnNames(StrEnum):
        """
        Enum containing all the column names in the public index table.
        """
        root_path_hash_tag = "root_path_hash_tag"
        relative_path_hash_tag = "rel_path_hash_tag"
        filename_hash_tag = "filename_hash_tag"
        absolute_file_path_hash_tag = "absolute_file_path_hash_tag"
        file_content_hash_tag = "file_content_hash_tag"
        creation_time = "creation_time"
        last_modification_time = "last_modification_time"
        file_size = "file_size"

    ##################################################################################################

    def __init__(self, database_config: DatabaseConfigMixin, public_index_table_name: str = "pub_index_table"):
        super().__init__(database_config, table_name=public_index_table_name)

    ##################################################################################################

    def reset(self):
        self.drop_all_tables_and_views()
        self._create_index_table()

    ##################################################################################################

    def _create_index_table(self):
        q = """
          CREATE TABLE IF NOT EXISTS {tbl} 
          (                
              {prooth} TEXT NOT NULL,
              {prelh} TEXT NOT NULL,
              {fnameh} TEXT NOT NULL,
              {pabsfnameh} TEXT NOT NULL,
              {fconth} TEXT NOT NULL,

              {ctime} TEXT NOT NULL,
              {mtime} TEXT NOT NULL,
              {fsize} INTEGER NOT NULL,

              PRIMARY KEY
              (
                  {prooth},
                  {prelh},
                  {fnameh}
              )
          )
          """.format(
            tbl=self.table_name(),

            prooth=PublicDataBase.PublicIndexTableColumnNames.root_path_hash_tag.value,
            prelh=PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value,
            fnameh=PublicDataBase.PublicIndexTableColumnNames.filename_hash_tag.value,
            pabsfnameh=PublicDataBase.PublicIndexTableColumnNames.absolute_file_path_hash_tag.value,
            fconth=PublicDataBase.PublicIndexTableColumnNames.file_content_hash_tag.value,

            ctime=PublicDataBase.PublicIndexTableColumnNames.creation_time.value,
            mtime=PublicDataBase.PublicIndexTableColumnNames.last_modification_time.value,
            fsize=PublicDataBase.PublicIndexTableColumnNames.file_size.value
        )
        self.cursor().execute(q)


##################################################################################################

class IndexDataBases(object):
    ##################################################################################################

    def __init__(self, private_db_config: DatabaseConfigMixin,
                 public_db_config: DatabaseConfigMixin, **kwargs):
        self.private_db = PrivateDataBase(private_db_config, **kwargs)
        self.public_db = PublicDataBase(public_db_config, **kwargs)
        self._dbs = [self.private_db, self.public_db]

        # Attach the public database to the private db. to allow db-spanning queries by use of the private db cursor.
        self.private_db.cursor().execute("ATTACH DATABASE \"{db}\" AS public".format(db=self.public_db.database_path()))

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
            self._insert_files_in_private_table(files)

            print("Storing data sets to public database ...")

            q = """
            INSERT INTO 
                {pub_tbl}
            SELECT 
                {prooth},
                {prelh},
                {fnameh},
                {pabsfnameh},
                {fconth},
                {ctime},
                {mtime},
                {fsize}
            FROM
                {priv_tbl} 
            """.format(
                pub_tbl="public.{}".format(self.public_db.table_name()),

                prooth=PublicDataBase.PublicIndexTableColumnNames.root_path_hash_tag.value,
                prelh=PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value,
                fnameh=PublicDataBase.PublicIndexTableColumnNames.filename_hash_tag.value,
                pabsfnameh=PublicDataBase.PublicIndexTableColumnNames.absolute_file_path_hash_tag.value,
                fconth=PublicDataBase.PublicIndexTableColumnNames.file_content_hash_tag.value,

                ctime=PublicDataBase.PublicIndexTableColumnNames.creation_time.value,
                mtime=PublicDataBase.PublicIndexTableColumnNames.last_modification_time.value,
                fsize=PublicDataBase.PublicIndexTableColumnNames.file_size.value,

                priv_tbl=self.private_db.table_name())
            self.private_db.connection().execute(q)

        print("Storing data to database done.")

    ##################################################################################################

    def _insert_file_in_private_table(self, file: FileType):
        if file is None:
            return
        q = """
        INSERT INTO 
            {tbl}
        VALUES 
            {values}
        """.format(tbl=self.private_db.table_name(),
                   values=convert_file_type_to_sql_entry(file))
        try:
            self.private_db.cursor().execute(q)
        except sqlite3.Error as e:
            print(q)
            print(convert_file_type_to_sql_entry(file))
            raise e
        except object:
            assert False

    ##################################################################################################

    def _insert_files_in_private_table(self, files: List[FileType]) -> None:
        if files is None:
            return
        q = """
        INSERT INTO 
            {tbl}
        VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.format(tbl=self.private_db.table_name())
        try:
            self.private_db.cursor().executemany(q, convert_file_type_list_to_tuple_list(files))
        except sqlite3.Error as e:
            print(q)
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
            "ATTACH DATABASE \"{db}\" AS index_db".format(db=self.index_db.database_path()))

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
            "DROP TABLE IF EXISTS {}".format(UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME))
        self.evaluation_db.cursor().execute(
            "DROP TABLE IF EXISTS {}".format(UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME))

    ##################################################################################################

    def _create_table_of_unique_files(self):
        q = """
        CREATE TABLE IF NOT EXISTS {tbl} 
        ( 
            {fht} TEXT NOT NULL,
            {cnt} INTEGER NOT NULL
        )
        """.format(tbl=UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME,
                   fht=PublicDataBase.PublicIndexTableColumnNames.file_hash_tag.value,
                   cnt="cnt")
        self.evaluation_db.cursor().execute(q)

    ##################################################################################################

    def _insert_into_table_of_unique_files(self):
        q = """
        INSERT INTO 
            {unique_files_tbl}
        SELECT 
            {file_hash_tag}, COUNT({file_hash_tag}) AS {cnt}
        FROM 
            {pub_index_tbl}
        GROUP BY 
            {file_hash_tag}
        """.format(
            unique_files_tbl=UniqueFileFolderEvaluator.UNIQUE_FILES_TABLE_NAME,
            file_hash_tag=PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value,
            cnt="cnt",
            pub_index_tbl="index_db.{}".format(self.index_db.table_name()))
        self.evaluation_db.cursor().execute(q)

    ##################################################################################################

    def _create_table_of_unique_folders(self):
        q = """
        CREATE TABLE IF NOT EXISTS {tbl} 
        (
            {rpth} TEXT NOT NULL,
            {cnt} INTEGER NOT NULL
        )
        """.format(
            tbl=UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME,
            rpth=PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value,
            cnt="cnt")
        self.evaluation_db.cursor().execute(q)

    ##################################################################################################

    def _insert_into_table_of_unique_folders(self):
        q = """
        INSERT INTO 
            {unique_unique_folders_tbl}
        SELECT 
            {path_hash_tag}, COUNT({path_hash_tag})
        FROM 
            {pub_index_tbl}
        GROUP BY 
            {path_hash_tag}
        """.format(
            unique_unique_folders_tbl=UniqueFileFolderEvaluator.UNIQUE_FOLDERS_TABLE_NAME,
            path_hash_tag=PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value,
            pub_index_tbl="index_db.{}".format(self.index_db.table_name()))
        self.evaluation_db.cursor().execute(q)


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
            """
            CREATE TABLE IF NOT EXISTS {tbl}
            (
                {rpht} TEXT NOT NULL, 
                {fht} TEXT NOT NULL, 
                {cnt} INTEGER NOT NULL
            )
            """.format(
                tbl=ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_TABLE_NAME,
                rpht=PrivateDataBase.PrivateIndexTableColumnNames.relative_path_hash_tag.value,
                fht=PrivateDataBase.PrivateIndexTableColumnNames.file_hash_tag.value,
                cnt="cnt")
        )

    ##################################################################################################

    def _insert_into_table_of_expected_folder_structure(self):
        q = """ 
        INSERT INTO 
            {expected_folder_structure_table}
        SELECT  
            DISTINCT rel_path_hash_tag, file_hash_tag, cnt 
        FROM
            (SELECT 
                B.{rpht}, A.{fht}, A.{cnt}
            FROM 
                {unique_files_table} AS A
            LEFT JOIN
            {index_tbl} AS B ON A.file_hash_tag= B.file_hash_tag) 
        """.format(
            expected_folder_structure_table=ExpectedFolderStructureEvaluator.EXPECTED_FOLDER_STRUCTURE_TABLE_NAME,
            rpht=PublicDataBase.PublicIndexTableColumnNames.relative_path_hash_tag.value,
            fht=PublicDataBase.PublicIndexTableColumnNames.file_hash_tag.value,
            cnt="cnt",
            unique_files_table="unique_files",
            index_tbl="index_db.{}".format(self.index_db.table_name()))
        self.evaluation_db.cursor().execute(q)


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
