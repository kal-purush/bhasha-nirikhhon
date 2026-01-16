var app = require('./server.js');
var supertest = require('co-supertest');
var expect = require('chai').expect;

describe('OAuth', () => {
    describe('Facebook', () => {
        it('should redirect to Facebook on connect', function *(){
            var res = yield supertest(app).get('/connect/facebook').expect(302).end();
            expect(res.header.location).to.contain('https://www.facebook.com/dialog/oauth');
        });
    });
    describe('Twitter', () => {
        it('should redirect to Twitter on connect', function *(){
            var res = yield supertest(app).get('/connect/twitter').expect(302).end();
            expect(res.header.location).to.contain('https://api.twitter.com/oauth');
        });
    });
});