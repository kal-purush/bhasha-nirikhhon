import { getEnvValue } from '@utils'
import { Sequelize } from 'sequelize'

export const database = new Sequelize(getEnvValue('DB_DATABASE'), getEnvValue('DB_USER'), getEnvValue('DB_PASSWORD'), {
  dialect: getEnvValue('DB_DIALECT'),
  host: getEnvValue('DB_HOST'),
  port: getEnvValue('DB_PORT'),
})