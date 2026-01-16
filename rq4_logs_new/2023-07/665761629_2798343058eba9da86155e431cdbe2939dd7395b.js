module.exports = {
  routes: [
    {
      method: 'GET',
      path: '/arts/:slug',
      handler: 'art.findOne',
      config: {
        auth: false
      }
    }
  ]
}