import psycopg2
from pyspark.sql import DataFrame
from synchronize_to_database.config import DBConfig


db_cols = [
    "id", "title", "industry", "company_name", "education_level", "employment_type", "gender",
    "address", "description", "deadline", "experience_required", "yoe_min", "yoe_max", "post_date",
    "salary" , "salary_min", "salary_max", "skill", "level", "created_at", "updated_at"
]

class PostgreSQL:
    def __init__(self):
        self.conn = self.connect_to_db()
        self.cur = self.conn.cursor()

    def connect_to_db(self):
        return psycopg2.connect(
            host = DBConfig.host or "localhost",
            port = DBConfig.port or 5432,
            user = DBConfig.user,
            password = DBConfig.password,
            database = DBConfig.database
        )
    
    def create_table_if_not_exist(self, table):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS your_table_name (
                id VARCHAR(255),
                title TEXT,
                industry VARCHAR(255),
                company_name VARCHAR(255),
                education_level VARCHAR(255),
                employment_type VARCHAR(255),
                gender VARCHAR(255),
                address VARCHAR(255),
                description TEXT,
                deadline DATE,
                experience_required VARCHAR(255),
                yoe_min INTEGER,
                yoe_max INTEGER,
                post_date DATE,
                salary VARCHAR(255),
                salary_min NUMERIC,
                salary_max NUMERIC,
                skill TEXT,
                level VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        self.cur.execute(create_table_query)
        self.conn.commit()


    def check_if_exist_id(self, id):
        sql_query = f"SELECT * FROM {DBConfig.table} WHERE id = {id}"
        self.cur.execute(sql_query)
        data = self.cur.fetchall()
        if data:
            return True
        return False

    def insert_row(self, row, table):
        sql_query = f"""
            INSERT INTO {table} 
                ({",".join([col for col in db_cols if row[col]])})
            VALUES
                {tuple(row[col] for col in db_cols if row[col])}
            WHERE id = {row["id"]}
        """
        self.cur.execute(sql_query)


    def update_row(self, row, table):
        sql_query = f"""
            UPDATE {table}
            SET {", ".join([f"{col} = {row[col]}" for col in db_cols if row[col] is not None])}
            WHERE id = {row["id"]}
        """
        self.cur.execute(sql_query)


    def upsert_row_to_db(self, row, table):
        operation = "update" if self.check_if_exist(row["id"]) else "insert"
        if operation == "insert":
            self.insert_row(row, table)
        else:
            self.update_row(row, table)


    def upsert_wh_to_db(
        self,
        warehouse_df: DataFrame,
        table: str
    ):          
        warehouse_df.foreach(lambda row: self.upsert_row_to_db(row, table))
        self.conn.commit()
        self.conn.close()
