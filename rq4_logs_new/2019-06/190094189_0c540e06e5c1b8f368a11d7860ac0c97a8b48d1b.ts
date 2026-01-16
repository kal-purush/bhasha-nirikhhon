#!/usr/bin/env node

import { ArgumentParser }   from 'argparse'
import updateNotifier     from 'update-notifier'
import pkgUp                from 'pkg-up'

import { jqFile }       from '../src/jq'
import { resolveFile }  from '../src/resolve-file'
import { saveFile }     from '../src/save-file';

import {
  VERSION,
}               from '../src/'

async function checkUpdate() {
  const pkgFile   = await pkgUp({ cwd: __dirname })
  if (!pkgFile) {
    throw new Error('package.json not found!')
  }

  const pkg = require(pkgFile)
  const updateCheckInterval = 1000 * 60 * 60 * 24 * 7  // 1 week

  const notifier = updateNotifier({
    pkg,
    updateCheckInterval,
  })

  notifier.notify()
}

async function main(args: Args): Promise<number> {
  checkUpdate().catch(console.error)

  const file   = await resolveFile(args.path)
  const result = await jqFile(args.filter, file)

  if (args.inplace) {
    await saveFile(result, file)
  } else {
    console.info(result)
  }

  return 0
}

interface Args {
  filter  : string,
  inplace : boolean,
  path    : string
}

function parseArguments(): Args {
  const parser = new ArgumentParser({
    prog        : 'pkg-jq',
    version     : VERSION,
    addHelp     : true,
    description : 'Node.js Package jq Utility',
    epilog      : `Exmaple: pkg-jq -i '.publishConfig.tag="next"'`,
  })

  parser.addArgument(
    [ 'filter' ],
    {
      help: `jq filter`,
    },
  )

  parser.addArgument(
    [ 'path' ],
    {
      help: [
        'npm module subdirectory,',
        'or a json file.',
        'default: $PWD',
      ].join('\n'),
      nargs: '?',
      defaultValue: process.cwd(),
    },
  )

  parser.addArgument(
    [ '-i', '--in-place' ],
    {
      help: 'edit files in place',
      action: 'storeConst',
      constant: true,
      defaultValue: false,
      dest: 'inplace',
    },
  )

  return parser.parseArgs()
}

process.on('warning', (warning) => {
  console.warn(warning.name);    // Print the warning name
  console.warn(warning.message); // Print the warning message
  console.warn(warning.stack);   // Print the stack trace
});

main(parseArguments())
.then(process.exit)
.catch(e => {
  console.error(e)
  process.exit(1)
})