import cx_Oracle

def conectar_banco():
    dsn = cx_Oracle.makedsn("localhost", "1521", service_name="XE")  # Substitua "XE" pelo seu service name, se necessário
    conexao = cx_Oracle.connect(user="system", password="123456", dsn=dsn)  # Substitua "seu_usuario" e "sua_senha"
    return conexao