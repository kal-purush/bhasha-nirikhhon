'use strict'

import request from 'request-promise'
import config from '~/config/config'

const createLocation = async(data) => {
  var options = {
    method: 'POST',
    uri: config.api.location,
    body: data,
    json: true
  }
  return await request(options)
}

export default createLocation