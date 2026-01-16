import mocha = require('mocha');
import assert = require('assert');
import * as supertest from 'supertest';
import app from '../app/app';
import http = require('http');
import * as testData from './test.data';

let request: supertest.SuperTest<supertest.Test>;
let server: http.Server;

before(() => {
    request = supertest('http://localhost:8080/api');
    server = app.listen(8080, 'localhost');
})

after(() => {
    server.close();
})

describe('Test book router api', () => {
    it('should respond with json', (done) => {
        request.get('/book')
            .expect('Content-Type', /json/, done)
    })
    it('should return json array equal test data',(done) => {
        request.get('/book')
            .expect((res: supertest.Response) => {
                assert.equal(res.body,testData.bookData)
            })
            .end((err) => {
                if(err) done(err);
                else done();
            })
    })
})