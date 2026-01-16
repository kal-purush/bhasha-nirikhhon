const fs = require('fs')

const jsonServer = require('json-server')
const server = jsonServer.create()
const fetch = require('node-fetch')

const _ = require('lodash')

const PORT = 9090
const baseUrl = 'http://localhost:' + PORT

const jsonPath = {
  USERS : './registry/users.json',
  NS : './registry/ns.json',
}
let DATA = {}
_.map(jsonPath, (path, data) => {
  DATA = Object.assign({}, DATA, JSON.parse(fs.readFileSync(path)))
})
const router = jsonServer.router(DATA)

const middlewares = jsonServer.defaults()

server.use(middlewares)
server.use(jsonServer.bodyParser)

// server.use(bodyParser.text({type: 'application/x-www-form-urlencoded'}))
// server.use(bodyParser.json())

server.get('/ns', (req, res) => {
  const queryNs = req.query.ns
  fetch(`${baseUrl}/namespace`)
      .then(res => {
        return res.json()
      })
      .then(json => {
        let subJson
        if(queryNs) {
          let nsHierarchy = queryNs.split('.').reverse()
          subJson = findNsRecurse(json, nsHierarchy)
        } else {
          subJson = json
          // res.send(subJson)
        }
        res.send({
          httpstatus: 200,
          msg: 'OK',
          data: subJson
        })
      })
})
function findNsRecurse(namespace, nsHierarchy, level = 0) {
  if(_.isEmpty(nsHierarchy) || level === nsHierarchy.length) return namespace
  if(!Array.isArray(namespace)) namespace = [namespace]
  for(let i = 0; i < namespace.length; i++) {
    if(namespace[i].name === nsHierarchy[level] && level !== nsHierarchy.length - 1) {
      return findNsRecurse(namespace[i].children, nsHierarchy, level + 1)
    } else if(namespace[i].name === nsHierarchy[level]) {
      return namespace[i]
    }
  }
  return {}
}

server.use(jsonServer.rewriter({
  '/perm/user*': '/users$1',
  '/perm/group/list*': '/group$1',
  '/event/status*': '/status$1',
  '/custom/sa*': '/sa$1',
  '/custom/linkstats*': '/linkstats$1',
  '/deploy\\?ns=:ns&pname=:pname': '/deploy',
  '/token/check*': '/check$1',
  '/package/*': '/package',
  '/log*': '/log'
}))

server.use((req, res, next) => {
  const { method, url }= req
  if(method === 'POST') {
    if(url.startsWith('/user/signin')) {
      sendSigninRes(req, res)
    } else if(url.startsWith('/query2')) {
      sendQuery2Res(res)
    } else {
      sendResError(res)
    }
  } else if(method === 'PUT' || method === 'DELETE') {
    sendResError(res)
  } else {
    if(url.startsWith('/resource/search')) {
      sendResError(res)
    } else {
      next()
    }
  }
})

router.render = (req, res) => {
  const { url, method } = req

  if(url.startsWith('/users?username=') && !url.includes('&password')) {
    renderJsonp(req, res, 1)
  } else if(url.startsWith('/deploy')) {
    renderJsonp(req, res, 4)
  } else if(url.startsWith('/query?')) {
    renderJsonp(req, res, 0)
  } else if(url.startsWith('/namespace')) {
    renderJsonp(req, res, 0)
  } else if((url.startsWith('/resource') || url.startsWith('/status'))) {
    renderJsonp(req, res, 3)
  } else if(url.startsWith('/check')) {
    renderJsonp(req, res, 5)
  } else if(url.startsWith('/log')) {
    renderJsonp(req, res, 6)
  } else {
    renderJsonp(req, res)
  }
}
server.use(router)

// 处理服务器出错
server.use(function (err, req, res, next) {
  console.error(err.stack);
  sendResError(res, 'Something wrong')
})

server.listen(PORT, () => {
  console.log(`Json server is running, and the port is ${PORT}`)
})

// {key1: val1, key2: val2...} => ?key1=val1&key2=val2
function convertJsonToPath(obj) {
  let path = '?'
  for(let key in obj) {
    path += (`${key}=${obj[key]}&`)
  }
  if(path.endsWith('&') > 1) path = path.substring(0, path.length -1)
  return path
}

// 包装 json-server 自动返回的数据
// type: 0 -> 直接返回；1 -> 主要用于登录；3 -> /resource?type=xxx； 5 -> 500 not support, 6 -> 包含了一层data，且直接返回data里的
function renderJsonp(req, res, type = 2, status = 200) {
  let data = res.locals.data
  switch (type) {
    case 0:
      // data = res.locals.data
      break
    case 1:
      if(Array.isArray(data)) {
        data = data.length ? data[0] : []
      }
      if(_.isEmpty(data)) {
        status = 401
      }
      data = {
        httpstatus: status,
        msg: 'OK',
        data: data
      }
      break
    case 2:
      data = {
        httpstatus: status,
        msg: 'OK',
        data: data
      }
      break
    case 3:
      let subData = data[0].data
      data = {
        httpstatus: status,
        msg: 'OK',
        data: subData
      }
      break
    case 4:
      data = {
        httpstatus: status,
        msg: 'OK',
        data: data.data
      }
      break
    case 5:
      sendResError(res)
      break
    case 6:
      data = data.data
      break
    default:
      data = res.locals.data
      break
  }
  res.status(status).jsonp(data)
}

// 将post请求的 /user/signin，改为get /users请求
function sendSigninRes(req, res) {
  fetch(`${baseUrl}/users` + convertJsonToPath(req.body))
      .then(res => {
        return res.json()
      })
      .then(json => {
        if(!_.isEmpty(json.data)) {
          json.data = json.data[0]
          json.data.user = json.data.token = json.data.username
          res.send(json)
        } else {
          sendResError(res, 'User does not exist or too many entries returned: 0')
        }
      })
}

// 将post请求的 /query2 接口，转为get请求
function sendQuery2Res(res) {
  fetch(`${baseUrl}/query2`)
      .then(res => {
        return res.json()
      })
      .then(json => {
        res.send(json)
      })
}

// 出错时的请求响应
function sendResError(res, msg = 'Not support with the mock data') {
  let data = {
    httpstatus: 500,
    msg: msg,
    data: null
  }
  res.status(500).send(data)
}