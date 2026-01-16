#!/usr/bin/env node

const path = require('path')
const config = require('./config.json')
const clearDir = require('./src/lib')

const argv = process.argv.slice(2)

const log = console.log

const DIST_DIR = path.resolve(__dirname, argv[0] || config.dist_dir_path)
const LIMIT_SIZE = argv[1] || config.dist_dir_size
const NODE_MODULES = argv[2] || config.dist_dir_name

let isHelp = false

argv.forEach(arg => {
  if (arg.match(/^(-+|\/)(h(elp)?|\?)$/)) isHelp = true
})

if (isHelp) {
  log('')
  log('useage: cnm <path> <limit> <distDir>')
  log('')
  log('  delete the specified folder!')
  log('')
  process.exit(1)
}

clearDir(DIST_DIR, LIMIT_SIZE, NODE_MODULES)