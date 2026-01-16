import { Sequelize } from 'sequelize';
import dotenv from 'dotenv';

dotenv.config();

const { POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT } = process.env;

console.log(`Connecting to database with the following details:
  User: ${POSTGRES_USER}
  Database: ${POSTGRES_DB}
  Port: ${POSTGRES_PORT}
`);

const sequelize = new Sequelize(POSTGRES_DB!, POSTGRES_USER!, POSTGRES_PASSWORD!, {
	host: 'localhost',
	dialect: 'postgres',
	port: Number(POSTGRES_PORT),
	logging: true,
	pool: {
		max: 5,
		min: 0,
		acquire: 30000,
		idle: 10000,
	},
	define: {
		timestamps: true,
		createdAt: 'created_at',
		updatedAt: 'updated_at',
	},
});

sequelize
	.sync({ force: false }) // создаем таблицы, если они еще не созданы.
	.then(() => console.log('Tables created successfully'))
	.catch(error => console.error('Failed to create tables:', error));

export default sequelize;