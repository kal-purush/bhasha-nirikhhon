import { DatabaseClient, DevModeler, Modeler, SequelizeClient } from 'vineyard-ground'
import { FullConfig } from '../config/config-types'
import { StrictCollection } from 'vineyard-data/modern'

export interface Model {
  ground: Modeler
}

export function createModel (
  dbConfig: FullConfig['database'],
  schema: any = {},
  client: DatabaseClient = new SequelizeClient(dbConfig)
): Model {
  const modeler = !dbConfig.devMode
    ? new Modeler(schema, client)
    : new DevModeler(schema, client)

  return Object.assign({
    ground: modeler,
    db: modeler.getLegacyDatabaseInterface()
  }, modeler.collections) as any
}