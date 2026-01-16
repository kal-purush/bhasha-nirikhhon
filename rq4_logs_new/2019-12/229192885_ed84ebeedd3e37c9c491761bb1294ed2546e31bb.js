const { expect } = require('chai')
const supertest = require('supertest')
const app = require('../app')

describe('GET /generate endpoint', () => {
    it('should generate an array of 5', () => {
        return supertest(app)
            .get('/generate')
            .query({ n: 5 })
            .expect(200)
            .expect('Content-Type', /json/)
            .then(res => {
                expect(res.body).to.be.an('array')
                expect(res.body).to.have.lengthOf.at.least(1)
                expect(res.body).to.include(5)
            })
    })
})