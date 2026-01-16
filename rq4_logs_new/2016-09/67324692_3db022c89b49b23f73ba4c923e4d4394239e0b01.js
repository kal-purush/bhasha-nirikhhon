const url     = require('url')
const body    = require('body/json')
const through = require('through2')
const rpc     = require('blue-frog')

module.exports = Dispatcher

function Dispatcher (prefix) {
    if (!(this instanceof Dispatcher))
        return new Dispatcher(prefix)

    const PREFIX = '/'
    if (!prefix) prefix = {prefix: PREFIX}
    if (! prefix.prefix) prefix.prefix = PREFIX

    this.prefix  = prefix.prefix
    this.methods = {}
}

function hander (req, res) {
    const me  = this
    const flg = req.method.toUpperCase() === 'POST' &&
              url.parse(req.url).pathname === this.prefix

    if (! flg) return false

    body(req, res, (err, result) => {
        const batch = rpc.response.BatchStream('do JSON.stringify')
        const cont  = []

        batch.on('error', err => console.log(err)) // invalid response
        .pipe(through.obj((json, _, done) => {
            res.setHeader('content-type', 'application/json')
            res.setHeader('content-length', Buffer.byteLength(json))
            done(null, json)
        }))
        .pipe(res)

        if (err) { // json.parse error
            console.error(err)
            return batch.end(rpc.response.error(
                     null, JsonRpcError.ParseError(err)))
        }

        rpc.request.ParseStream(result).on('error', err => {
            // json-rpc 2.0 pare error
            console.error(err)
            batch.write(rpc.response.error(
                    null, JsonRpcError.InvalidRequest(err)))
        })
        .pipe(through.obj((req, _, done) => {
            var api = me.methods[req.method]
            if (typeof api !== 'function') {
                var err = new Error('"' + req.method + '" not found')
                console.error(err)
                return done(null
                         , rpc.response.error(
                             req.id || null
                           , JsonRpcError.MethodNotFound(err)))
            }

            api(req.params || {}, cont.pop() || {}, (err, result) => {
                if (err) {
                    console.log(err)
                    return done(null, serverError(err, req.id))
                }

                result && cont.push(result)
                if (! req.id) done()
                else done(null, rpc.response(req.id, result))
            })
        }))
        .pipe(batch)

        function serverError (err, id) {
            return rpc.response.error(id || null
                    , new JsonRpcError(-32000, 'Server error', err))
        }
    })

    return true
}

Dispatcher.prototype.method = function (method, onMethod) {
    this.methods[method] = onMethod
    return this
}

Dispatcher.prototype.install = function (server) {
    const me = this
    const EVENT_NAME   = 'request'
    const oldListeners = server.listeners(EVENT_NAME).slice(0)
    const newListener  = function (req, res) {
        if (!(hander.apply(me, arguments))) {
            for (var i = 0; i < oldListeners.length; i++) {
                oldListeners[i].apply(me, arguments)
            }
            return false
        }
        return true
    }

    server.removeAllListeners(EVENT_NAME)
    server.addListener(EVENT_NAME, newListener)
}