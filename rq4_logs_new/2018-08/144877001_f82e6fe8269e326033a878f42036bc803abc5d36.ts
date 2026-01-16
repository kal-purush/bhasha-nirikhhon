import { PostgresConnectionOptions } from 'typeorm/driver/postgres/PostgresConnectionOptions'

const typeOrmConfig: PostgresConnectionOptions = {
  type: 'postgres',
  host: 'localhost',
  port: 5432,
  username: 'typeormtest2',
  password: 'password',
  database: 'typeormtest2',
  synchronize: true,
  logging: true,
  entities: [__dirname + '/models/*.js']
}
export { typeOrmConfig }