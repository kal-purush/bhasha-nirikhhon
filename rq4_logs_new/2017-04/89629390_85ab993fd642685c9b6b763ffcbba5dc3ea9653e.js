'use strict'
const http = require('http')
const url = require('url')
const rLib = require('response-helper-functions')
const Pool = require('pg').Pool;
const microServices = [
  require('../permissions-maintenance/permissions-maintenance.js'),
  require('../permissions-audit/audit.js'),
  require('../permissions-migration/permissions-migration.js'),
  require('../teams/teams.js'),
  require('../folders/folders.js')
]

const config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
}
const pool = new Pool(config)

const COMPONENT_NAME = 'permissions-management'

function log(functionName, text) {
  console.log(Date.now(), COMPONENT_NAME, functionName, text)
}

function requestHandler(req, res) {
  for (var i=0; i<microServices.length; i++) {
    var microService = microServices[i]
    var paths = microServices[i].paths
    for (var j=0; j<paths.length; j++)
      if (req.url.startsWith(paths[j]))
        return microService.requestHandler(req, res)
  }
  rLib.notFound(res, `allinone: //${req.headers.host}${req.url} not found`)
}

function start() {
  var count = 0
  for (let i=0; i<microServices.length; i++) {
    var microService = microServices[i]
    microService.init(function(err) {
    if (err)
      log(`failed to init microservice ${i}`, err)
    else 
      if (++count == microServices.length) {
        var port = process.env.PORT
        http.createServer(requestHandler).listen(port, function() {
          log('start', `server is listening on ${port}`)
        })
      }
    }, pool)
  }
}

start()