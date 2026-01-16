'use strict'

const config = require('../config.js')
const store = require('../store.js')

const getAll = function () {
  return $.ajax({
    url: config.apiUrl + '/videos',
    method: 'GET',
    headers: {
      contentType: 'application/json',
      Authorization: 'Token token=' + store.user.token
    }
  })
}

const getVideoByID = function (id) {
  return $.ajax({
    url: config.apiUrl + '/videos/' + id,
    method: 'GET',
    headers: {
      Authorization: 'Token token=' + store.user.token
    }
  })
}

module.exports = {
  getAll,
  getVideoByID
}