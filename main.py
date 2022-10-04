import sqlite3 as sql

class BetterSQL:
    def __init__(self, connection_string: str, debug: bool = None) -> None:
        self.database = self._connect(connection_string)
        self.cursor = self.database.cursor()
        self.debug = debug or False

    def _connect(self, connection_string: str) -> sql.Connection:
        return sql.connect(connection_string)

    def _commit(self) -> None:
        """
        Commits all pending transactions to the database
        Logs all changes if debug is enabled
        """
        if self.debug:
            for transaction in self.database.iterdump():
                print(transaction)
        self.database.commit()

    def create_table(self, table_name: str, columns: list[str]) -> bool:
        """
        INPUT:    Name of the table and an array of column names
        FUNCTION: Creates an SQL table with a name and columns.
        OUTPUT:   Returns True if table was created or updated, False if it already exists or was unmodified.
        """

        if self.table_exists(table_name):
            # Get the columns in the table
            table_columns = self.get_table(table_name)
            if columns == table_columns:
                return False

            # Get the columns in the existing table,
            # and find which need to be added or removed
            to_create, to_delete = [], []
            for column in columns:
                if column not in table_columns:
                    to_create.append(column)
            for column in table_columns:
                if column not in columns:
                    to_delete.append(column)

            
        else:
            columns = ', '.join(columns)
            self.cursor.execute(f'CREATE TABLE {table_name} ({columns})')

        # Commit and save the transaction
        self._commit()
        return True

    def table_exists(self, table_name: str) -> bool:
        """
        INPUT:    Name of the table
        FUNCTION: Checks if the table exists
        OUTPUT:   Returns True if the table exists, False if it doesn't
        """

        self.cursor.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}"')
        return bool(self.cursor.fetchone())

    def get_columns(self, table_name: str) -> list[str]:
        """
        INPUT:    Name of the table
        FUNCTION: Gets the columns in the table
        OUTPUT:   Returns a list of column names
        """

        self.cursor.execute(f'PRAGMA table_info({table_name})')
        return [column[1] for column in self.cursor.fetchall()]



database = BetterSQL("main.db")
print(database.create_table("Testing", ["test", "test2"]))
