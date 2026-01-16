const kratos = require('@ory/kratos-client')

module.exports = () => async (cookie) => {
  const config = new kratos.Configuration({ basePath: process.env.IDP_URL })
  const client = new kratos.PublicApi(config)
  const session = await client.whoami(cookie)

  return session.data
}