const supertest = require('supertest');
const expect = require('chai').expect;

describe('REST API', () => {
    let server;

    before((done) => {
        require('../express/server');
        setTimeout(() => {
            server = supertest.agent('http://localhost:1337');
            done();
        }, 1000);
    });

    describe('REST /api/N1/users', function () {

        const user = {'id': 1, 'name': 'Anastasia', 'score': 1};
        const OK = 200;
        const ERROR = 400;
        let URL = '/api/N1/users';

        it('Тест на успешное создание пользователя', function (done) {
            server
                .post(URL).send(user).expect(OK, user)
                .end((err, res) => (err) ? done(err) : done());
        });

        it('Тест на неудачное создание пользователя', function (done) {
            let failUser = {'id': 1, 'score': 1};
            server
                .post(URL).send(failUser).expect(ERROR, {})
                .end((err, res) => (err) ? done(err) : done());
        });

        it('Тест на успешное удаление пользователя', function (done) {
            server
                .del(URL + '/1').expect(OK, user)
                .end((err, res) => (err) ? done(err) : done());
        });

        it('Тест на неудачное удаление пользователя', function (done) {
            server
                .del(URL + '/2').expect(ERROR, {})
                .end((err, res) => (err) ? done(err) : done());
        });
    });

});
