from datetime import datetime
import psycopg
from settings import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def upsert_item_query(table: str, item: dict) -> str:
    """
    Gera uma query de upsert (insere se não existe, atualiza se existe) no postgres.
    args:
        * table: nome da tabela no postgres
        * item: dicionário contendo as informações do item
    PS: table deve conter todos os campos das chaves de item, exceto created_at e updated_at.
    """
    insert = f"INSERT INTO {table}("
    values = "VALUES("
    on_conflict = "ON CONFLICT (id)\nDO UPDATE SET "
    for key, value in item.items():
        insert += f"{key},"

        if isinstance(value, str):
            values += f"'{value}',"
            on_conflict += f"{key} = '{value}',"
        else:
            values += f"{value},"
            on_conflict += f"{key} = {value},"

    insert += "updated_at)\n"
    values += f"NOW())\n"
    on_conflict += f"updated_at = NOW();"
    return insert + values + on_conflict


def select_non_inserted_pids_query(
    pid_errors_file: str = "pid_errors.log",
) -> str | None:
    query_pids = set()
    with open(pid_errors_file, "r", encoding="utf-8") as file:
        for product_id in file:
            product_id = product_id.strip()
            query_pids.add(f"('{product_id}')")
    if query_pids:
        query = "SELECT id\nFROM (VALUES \n"
        query += ", \n".join(query_pids)
        query += f") V(id) EXCEPT SELECT id FROM products WHERE updated_at BETWEEN '{datetime.today().strftime('%Y-%m-%d')}' AND NOW();"
        return query
    return "select id from products where id = '__________'"


class PostgresDB:
    def __init__(self) -> None:
        self.conninfo = f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}"

    async def upsert_item(self, table: str, item: dict) -> None:
        async with await psycopg.AsyncConnection.connect(self.conninfo) as aconn:
            async with aconn.cursor() as cur:
                await cur.execute(upsert_item_query(table, item))

    def select_non_inserted_ids(self, pid_errors_file: str) -> list:
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cur:
                cur.execute(select_non_inserted_pids_query(pid_errors_file))
                non_inserted_ids = cur.fetchall()
                conn.commit()
                return [pid[0] for pid in non_inserted_ids]