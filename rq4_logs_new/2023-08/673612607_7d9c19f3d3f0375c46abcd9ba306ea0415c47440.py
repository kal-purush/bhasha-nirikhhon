import psycopg2
import psycopg2.extras as extras
import pandas as pd
import requests
import json

class DatabaseManager:
    def __init__(self, database, host, user, password, port="5432"):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.conn = None
        self.cur = None

    # Chamado quando o bloco with é inicializado
    def __enter__(self):
        self.conn = psycopg2.connect(
            database=self.database,
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cur = self.conn.cursor()
        return self

    # Chamado quando o bloco with é finalizado
    def __exit__(self, exc_type, exc_value, traceback):
        if self.cur:
            self.cur.close()
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()

    def execute_values(self, df, table_name):
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query = f"INSERT INTO {table_name}({cols}) VALUES %s"
        
        try:
            extras.execute_values(self.cur, query, tuples)
            print("Valores inseridos com sucesso!")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            
    def create_or_replace_table(self, df, table_name, list_types):
        cols = ', '.join([f"{col} {type}" for col, type in zip(df.columns, list_types)])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
        self.cur.execute(query)
    
    def call_api(self, URL, payload):
        response = requests.post(url=URL, data=json.dumps(payload))

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Falha na solicitação POST à URL: {URL}")
            
    def connect_csv(self, archive, list_columns_filter=[]):
        data = pd.read_csv(archive)
        if list_columns_filter != []:  
            data = data[list_columns_filter]
        return data
    

def main():
    table_name = "stg_prontuario.Teste40"
    payload = {"code": "BR"}
    url = "https://api.statworx.com/covid"
    archive_csv = "database.csv"
    list_columns_filter = ["nome", "dt_nascimento", "cpf","nome_mae", "dt_atualizacao"]
    types_columns = ["VARCHAR(100) NOT NULL", "DATE NOT NULL", "INTEGER NOT NULL", "VARCHAR(100) NOT NULL", "TIMESTAMP"]

    db_manager = DatabaseManager(
        database="staging",
        host="localhost",
        user="postgres",
        password="123123123",
        port="5432"
    )

    # Gerenciador de contexto
    with db_manager:
        #data = db_manager.call_api(url, payload)
        data = db_manager.connect_csv(archive_csv, list_columns_filter)
        db_manager.create_or_replace_table(data, table_name, types_columns)
        db_manager.execute_values(data, table_name)
        
if __name__ == "__main__":
    main()