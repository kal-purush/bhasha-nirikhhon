import { create, sync, drop } from '../lib/commands/db'

const run = async (command: string, opts: string[]) => {
  if (command === 'create') { await create() }

  if (command === 'drop') { await drop() }

  if (command === 'sync') {
    if (opts.length > 0 && opts.includes('--force')) {
      await sync(true)
    } else {
      await sync()
    }
  }
}

const args: string[] = process.argv.slice(2)
const cmd = args.shift() || ''

run(cmd, args)
  .then(() => {
    console.log('Done')
    process.exit(0)
  })
  .catch((err) => {
    console.error(err)
    process.exit(1)
  })