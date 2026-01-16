import {Hook} from '@oclif/config'
import * as fs from 'fs-extra'
import * as path from 'path'

const hook: Hook<'init'> = async function(opts) {
  const file = path.join(opts.config.configDir, 'config.json')
  if (fs.existsSync(file)) return
  fs.mkdirSync(opts.config.configDir)
  fs.writeFileSync(file, JSON.stringify({url: 'http://localhost:9200'}))
}

export default hook