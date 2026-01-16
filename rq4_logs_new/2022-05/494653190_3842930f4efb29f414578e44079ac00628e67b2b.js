#! /usr/bin/env node

const fs = require('fs')
const path = require('path')

let genfile ={}
const genfilePath = path.resolve(process.cwd(), './genfile.js')
if(fs.existsSync(genfilePath)) {
  genfile = require(genfilePath)
}
require('../dist/index')(genfile)