const fetch = require('node-fetch')

function apiRequest (url, opts = {}) {
  let headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'User-Agent': 'http://localhost:3000'
  }

  const { method = 'GET', accessToken, body } = opts

  if (accessToken) {
    headers = { ...headers, 'Authorization': `Bearer ${accessToken}` }
  }

  console.log(`Making ${method} request to url=${url} with body ${body}`)

  return fetch(url, { method, body, headers })
    .then(res => res.ok
      ? res.json()
      : res.text().then(textBody => ({ error: textBody }))
    )
    .then(body => {
      console.log(`Received response with body=${JSON.stringify(body)}`)
      return body
    })
}

module.exports = { apiRequest }