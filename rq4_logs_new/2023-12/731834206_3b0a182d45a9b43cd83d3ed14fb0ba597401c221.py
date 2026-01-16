import config
import psycopg2

connection = psycopg2.connect(
    hostname = config.hostname,
    db = config.database,
    user = config.username,
    pwd = config.password,
    port = config.port    
)



connection.close()