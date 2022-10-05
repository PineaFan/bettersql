import sqlite3 as sql
import colours as colours

# Default SQL types accepted by SQL
sql_types = {
    None: "NULL",
    int: "INTEGER",
    float: "REAL",
    str: "STRING",
    bytes: "BLOB"
}


class Column:
    def __init__(self, name: str, column_type = str):
        self.name = name
        if isinstance(column_type, bool):
            column_type = int
        if column_type in list(sql_types.values()):
            column_type = list(sql_types.keys())[list(sql_types.values()).index(column_type)]
        assert column_type in list(sql_types), "Not a valid type"
        self.type = column_type


class BetterSQL:
    def __init__(self, connection_string: str, debug: bool = None) -> None:
        self.database = self._connect(connection_string)  # Connect to the database and initalise it
        self.cursor = self.database.cursor()  # The main cursor in the SQL database
        self.debug = debug or False  # If true, will print debug messages
        self.history = []  # A list showing all previous executed commands
        self.transaction = []  # A list of all pending transactions

    def _connect(self, connection_string: str) -> sql.Connection:
        """
        FUNCTION: Connects to a SQL database
        OUTPUT:   Returns an SQL connection object
        """

        return sql.connect(connection_string)

    def disconnect(self) -> None:
        """
        FUNCTION: Disconnects from the database
        """

        self.database.close()
        self._success("Disconnected from database")

    def _debug(self, message: str = None) -> None:
        """
        FUNCTION: Prints all pending transactions if debug is enabled
        """

        if message:
            if self.debug:
                print(f"{colours.cyan}[i] DEBUG:  {colours.clear} {message}")
            return
        if self.debug:
            for command in self.transaction:
                print(f"{colours.cyan}[i] DEBUG:  {colours.clear} {command[0]}")
                if command[2]:
                    print(f"{colours.cyan}[i] DEBUG:   {colours.clear}└> {command[2]}")
                if command[1] == False:
                    self._error(f"└> {command[3]}")
        else:
            for command in self.transaction:
                if command[1] == False:
                    self._error(f"{command[3]}")

    def _success(self, message: str = "Success") -> None:
        """
        INPUT:    Success message
        FUNCTION: Prints a success message if debug is enabled
        """

        if self.debug:
            print(f"{colours.green}[+] SUCCESS:{colours.clear} {message}")

    def _error(self, message: str = "An unknown error occurred", fatal: bool = False) -> None:
        """
        INPUT:    Error message
        FUNCTION: Prints an error message
        """

        start = f"{colours.red_dark}[!] FATAL:  {colours.red_dark}" if fatal else f"{colours.red}[X] ERROR:  {colours.clear}"
        print(f"{start} {message}")

    def _clean_string(self, text: any) -> any:
        """
        INPUT:    String to clean
        FUNCTION: Cleans a string so it can be used in an SQL command
        OUTPUT:   Returns the cleaned string
        """

        if not isinstance(text, str):
            return text
        return text.replace("'", "").replace('"', "")

    def execute(self, command: str, exclude: bool = False, message: str = None) -> bool:
        """
        INPUT:    SQL command
        FUNCTION: Execute an SQL command
        OUTPUT:   Returns True if the command was executed successfully
        """

        try:
            self.cursor.execute(command)
            if not exclude:
                self.transaction.append((command, True, message))
            return True

        # Immediate error occured
        except Exception as e:
            if not exclude:
                self.transaction.append((command, False, message, e))
            return False

    def _commit(self) -> None:
        """
        FUNCTION: Commits all pending transactions to the database
        """

        self._debug(f"{colours.blue}Committing the following changes:{colours.clear}")
        self._debug()
        self.history += self.transaction
        if False in [c[1] for c in self.transaction]:
            self.database.rollback()
            self._error("Transaction failed, rolling back\n", fatal=True)
            self.transaction = []
            return
        self.database.commit()
        self.transaction = []
        self._success(f"{colours.green}Transaction committed{colours.clear}\n")

    def create_table(self, table_name: str, columns: list[Column]) -> bool:
        """
        INPUT:    Name of the table and an array of column names
        FUNCTION: Creates an SQL table with a name and columns.
        OUTPUT:   Returns True if table was created or updated, False if it already exists or was unmodified.
        """

        if self.table_exists(table_name):
            # Get the columns in the table
            table_columns = self.get_columns(table_name)
            if [column.name for column in columns] == table_columns:
                return False

            # Get the columns in the existing table,
            # and find which need to be added or removed
            to_create, to_delete = [], []
            for column in columns:
                column = column.name
                if column not in table_columns:
                    to_create.append(column)
            for column in table_columns:
                if column not in columns:
                    to_delete.append(column)

            # Create the new columns
            for column_name in to_create:
                self.execute(
                    f'ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_types[columns[column_name].type]}'
                )
            # Delete the old columns
            for column_name in to_delete:
                self.execute(f'ALTER TABLE {table_name} DROP COLUMN {column_name}')

        else:
            self.execute(
                f"CREATE TABLE {table_name} (" + \
                ", ".join([f"{column.name} {sql_types[column.type]}" for column in columns]) + \
                ")"
            )

        self._commit()
        return True

    def delete_table(self, table_name: str) -> bool:
        """
        INPUT:    Name of the table
        FUNCTION: Deletes the table and all documents
        OUTPUT:   Returns True if the table was deleted
        """

        success = self.execute(f"DROP TABLE {table_name}")

        self._commit()
        return success

    def table_exists(self, table_name: str) -> bool:
        """
        INPUT:    Name of the table
        FUNCTION: Checks if the table exists
        OUTPUT:   Returns True if the table exists, False if it doesn't
        """

        self.execute(
            f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}"',
            message="Checking if table exists"
        )
        return bool(self.cursor.fetchone())

    def get_columns(self, table_name: str) -> list[Column]:
        """
        INPUT:    Name of the table
        FUNCTION: Gets the columns in the table
        OUTPUT:   Returns a list of column names
        """

        self.execute(f'PRAGMA table_info({table_name})', exclude=True)
        return [Column(column[1], column[2]) for column in self.cursor.fetchall()]

    def create_record(self, table_name: str, **kwargs) -> True:
        """
        INPUT:    Name of the table and a dictionary of column names and values
        FUNCTION: Creates a record in the table
        OUTPUT:   Returns True if the record was created
        """

        columns = ", ".join(list(kwargs))
        values = ", ".join([f'"{self._clean_string(value)}"' for value in kwargs.values()])
        success = self.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")

        self._commit()
        return success

    def get_all_records(self, table_name: str, **kwargs: dict[str, callable] | None) -> list[dict]:
        """
        INPUT:    Name of the table, and optionally a dictionary of column names, and values or functions to filter by
        FUNCTION: Gets all records in the table
        OUTPUT:   Returns a list of records
        """

        self.execute(f"SELECT * FROM {table_name}", exclude=len(kwargs) > 0)
        records = self.cursor.fetchall()
        columns = self.get_columns(table_name)
        records = [{columns[i].name: record[i] for i in range(len(columns))} for record in records]
        if kwargs:
            to_output = []
            for record in records:
                for column, value in kwargs.items():
                    if callable(value):
                        if not value(record[column]):
                            break
                    elif record[column] != value:
                        break
                else:
                    to_output.append(record)
            return to_output
        else:
            return records

    def delete_all_records(self, table_name: str, **kwargs: dict[str, callable] | None) -> bool:
        """
        INPUT:    Name of the table
        FUNCTION: Deletes all records in the table
        OUTPUT:   Returns True if the records were deleted
        """

        records = self.get_all_records(table_name, **kwargs)

        # Delete each record
        for record in records:
            self.execute(f"DELETE FROM {table_name} WHERE " + " AND ".join([
                f"{key} = '{value}'" for key, value in record.items()
            ]))


database = BetterSQL("main.db", debug=True)
database.delete_table("Testing")
# database.create_table("Testing", [])
# database.delete_table("ThisIsNotATable")
database.create_table("Testing", [Column("Name"), Column("Age", int)])
database.create_record("Testing", Name="John", Age=20)
database.create_record("Testing", Name="Bob", Age=30)
database.create_record("Testing", Name="Alice", Age=40)

print(database.delete_all_records(table_name="Testing", Age=lambda x : x > 25))