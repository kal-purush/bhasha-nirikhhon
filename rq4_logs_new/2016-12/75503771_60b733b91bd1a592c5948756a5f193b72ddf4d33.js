'use strict'

const Lab = require('lab')
const Code = require('code')

// Test shortcuts

const lab = exports.lab = Lab.script()
const describe = lab.describe
const it = lab.it
const expect = Code.expect

const server = require('../src/server')

describe('Example Server', () => {
  it('GET /', (done) => {
    server.inject('/', (res) => {
      expect(res.statusCode).to.equal(200)
      done()
    })
  })

  it('GET /junk', (done) => {
    server.inject('/junk', (res) => {
      expect(res.statusCode).to.equal(404)
      expect(res.result).to.equal({statusCode: 404, error: 'Not Found'})
      done()
    })
  })
})
