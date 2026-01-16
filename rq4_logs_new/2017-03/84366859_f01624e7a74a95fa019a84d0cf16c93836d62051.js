var chai = require('chai'),
    chaiHttp = require('chai-http');
var server = require('../server.js');
var should = chai.should;
var expect = chai.expect;
var user = require('./models/user.js');
chai.use(chaiHttp);


describe('Token authentication', function () {
    it('POST /auth/api/check - should return vaild if token is vaild', function (done) {
        user.username = "Test_auth";
        user.password = 'password1';
        var token;

        //first login and obtain token 
        chai.request(server.app)
            .post('/api/auth/login')
            .send(user)
            .end(function (err, res) {
                if (err) throw err;
                expect(err).to.be.null;
                token = JSON.parse(res.text);
                //authenticate on obtained token
                var json = { token: token['token'] };
                chai.request(server.app)
                    .post('/api/auth/check')
                    .send(json)
                    .end(function (err, res) {
                        if (err) throw err;
                        expect(err).to.be.null;
                        expect(res.text).to.equal('vaild');
                        done();

                    });

            });


    });
});


describe('Token authentication', function () {
    it('POST /auth/api/check - should return no matches if token is not in the database', function (done) {
        var token = "eyJhbGhyhyjgfukfukInR5cCI6IkpXVCJ9eyJpZCjfgjfjfgV0aCIsInBhc3N3b3JkIjoiJDJhJDEwJEZtZmNDamRhQ3dnalpOb02"
        var json = { token: token };
        chai.request(server.app)
            .post('/api/auth/check')
            .send(json)
            .end(function (err, res) {
                if (err) throw err;
                expect(err).to.be.null;
                expect(res.text).to.equal('No matchs');
                done();
            });

    });
});






