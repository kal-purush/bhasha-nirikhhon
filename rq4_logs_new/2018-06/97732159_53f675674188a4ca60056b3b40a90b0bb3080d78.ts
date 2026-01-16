import { FullConfig } from '../config/config-types'
import { createVillage } from '../src/village'
import { DevModeler } from 'vineyard-ground/source/modeler'
import { config } from '../config/config'

async function resetDb (config: FullConfig): Promise<void> {
  const village = createVillage(config)
  const model = village.model
  await (model.ground as DevModeler).regenerate()
  process.exit(0)
}

console.log('Resetting db')
resetDb(config)