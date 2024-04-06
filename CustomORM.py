import psycopg2


class CustomORM:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connected: bool = False
        self.conn = None
        self.cursor = None

    def connect(self):
        if not self.connected:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host,
                                         port=self.port)
            self.cursor = self.conn.cursor()
            self.connected = True

    def create_table(self, table_name: str, columns: dict):
        columns_str = ', '.join([f'{col} {data_type}' for col, data_type in columns.items()])
        query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})'
        self.cursor.execute(query)
        self.conn.commit()

    def drop_table(self, table_name: str):
        query = f'DROP TABLE IF EXISTS {table_name}'
        self.cursor.execute(query)
        self.conn.commit()

    def rename_table(self, old_table_name: str, new_table_name: str):
        query = f'ALTER TABLE {old_table_name} RENAME TO {new_table_name}'
        self.cursor.execute(query)
        self.conn.commit()

    def add_column(self, table_name: str, column_name: str, data_type: str):
        query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {data_type}'
        self.cursor.execute(query)
        self.conn.commit()

    def drop_column(self, table_name: str, column_name: str):
        query = f'ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}'
        self.cursor.execute(query)
        self.conn.commit()

    def change_column_dtype(self, table_name: str, column_name: str, new_data_type: str):
        query = f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_data_type}'
        self.cursor.execute(query)
        self.conn.commit()

    def select(self, table_name: str, columns: str = '*', conditions: str | None = None, limit: str | None = None,
               order_by: str | None = None) -> list[tuple]:
        query = f'SELECT {columns} FROM {table_name}'
        if conditions:
            query += f' WHERE {conditions}'
        if order_by:
            query += f' ORDER BY {order_by}'
        if limit:
            query += f' LIMIT {limit}'
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert(self, table_name: str, values: tuple):
        placeholders = ', '.join(['%s' for _ in range(len(values))])
        query = f'INSERT INTO {table_name} VALUES ({placeholders})'
        self.cursor.execute(query, values)
        self.conn.commit()

    def filter_like(self, table_name: str, column: str, pattern: str, columns: str = '*') -> list[tuple]:
        query = f'SELECT {columns} FROM {table_name} WHERE {column} LIKE %s'
        self.cursor.execute(query, (pattern,))
        return self.cursor.fetchall()

    def filter_ilike(self, table_name: str, column: str, pattern: str, columns: str = '*') -> list[tuple]:
        query = f'SELECT {columns} FROM {table_name} WHERE {column} ILIKE %s'
        self.cursor.execute(query, (pattern,))
        return self.cursor.fetchall()

    def filter_in(self, table_name: str, column: str, values: tuple, columns: str = '*') -> list[tuple]:
        placeholders = ', '.join(['%s' for _ in range(len(values))])
        query = f'SELECT {columns} FROM {table_name} WHERE {column} IN ({placeholders})'
        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def close_connection(self):
        if self.connected:
            self.conn.close()
            self.connected = False
