from database import Database

print('Starting...')

database = Database()

print(str(type(database)))

print('Database object created')

database.connect()

print('Database opened')

sql = ('SELECT count(ID) FROM tbl_players p WHERE FirstName = %s')
params = ('Brian', )
rs = database.query(sql, params)
for value in rs:
    print(str(value[0]) + ' players named ' + str(params[0]) + ' in database')

print('Database queried')

print(str(database.warnings))

print('Database warnings retrieved')

database.disconnect()

print('Database closed')

print('Finished!')