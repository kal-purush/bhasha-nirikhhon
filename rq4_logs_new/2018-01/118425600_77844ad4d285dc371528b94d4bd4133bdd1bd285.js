module.exports = {
  port: 3000,
  session: {
    secret: 'myblog',
    key: 'myblog',
    // maxAge: 2592000000
    maxAge: 3000000
  },
  mongodb: 'mongodb://localhost:27017/contentManage'
}