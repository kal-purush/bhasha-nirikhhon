/// <reference path="../typings/tsd.d.ts" />
<% if (props.handler == 'create_mongo') {
%> var Promise = require("bluebird");
var mongojs = require("mongojs");

var mongoUrl = process.env.MONGO_PORT_27017_TCP_ADDR ?
  "mongodb://#{process.env.MONGO_PORT_27017_TCP_ADDR}:#{process.env.MONGO_PORT_27017_TCP_PORT}" :
  process.env.MONGO_HANDLER_URI;

var mongoCollection = process.env.MONGO_HANDLER_COLLECTION;

var db = mongojs(mongoUrl, [mongoCollection]);
var coll : any = Promise.promisifyAll(db[mongoCollection]);
var findAsync : <T>(query: any) => Promise<T[]> = Promise.promisify(coll.find, coll)

interface IPosition {
  _id: string
  ticker: string
}
<% } %>

export function handle (log: logger.ILogger, cmd) {
  log.write({oper: "handle", status: "start", cmd: cmd})
}

exports.handle = handle;