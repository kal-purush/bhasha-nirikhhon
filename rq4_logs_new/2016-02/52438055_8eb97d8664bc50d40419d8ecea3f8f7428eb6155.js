'use strict'

function mapValues (obj, f) {
  let res = {}
  for (let key of Object.keys(obj)) {
    res[key] = f(obj[key], key, obj)
  }
  return res
}

function isObject (obj) {
  return obj && typeof obj === 'object'
}

const isEqual = require('deep-equal')

module.exports = { mapValues, isObject, isEqual }