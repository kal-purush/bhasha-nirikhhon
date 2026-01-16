import chai from 'chai';
import { describe, it } from 'mocha'
import faker from 'faker'
import chaiHttp from 'chai-http';
import app from '../../src/server';
import db from "../../src/database/models/index"
import helpers from '../../src/utils/helpers';

chai.use(chaiHttp);
chai.should();

describe("POST /api/v1/auth/login", () => {
    let email, name, password;
    before(async () => {
        email = faker.internet.email();
        name = faker.name.firstName();
        password = faker.finance.iban().substr(0, 15);
        await db.User.create({
            email,
            password: helpers.hashPassword(password),
            name
        });
    })

    after(async () => {
        await db.User.truncate();
    })

    it("should be able to login", (done) => {
        chai.request(app)
            .post('/api/v1/auth/login')
            .send({
                email,
                password
            })
            .end((err, res) => {
                res.should.have.status(200);
                res.should.be.an('object');
                done();
            })
    });

    it("should not be able to login if the email is incorrect", (done) => {
        chai.request(app)
            .post('/api/v1/auth/login')
            .send({
                email: "erty@hv.cm",
                password: "wwqer"
            })
            .end((err, res) => {
                res.should.have.status(401);
                res.should.be.an('object');
                done();
            });
    });

    it("should not be able to login if the password is incorrect", (done) => {
        chai.request(app)
            .post('/api/v1/auth/login')
            .send({
                email: "erty@hv.cm",
                password: "wwqer"
            })
            .end((err, res) => {
                res.should.have.status(401);
                done();
            });
    });

    it("should not be able to login if the email or the password is incorrect ", (done) => {
        chai.request(app)
            .post('/api/v1/auth/login')
            .send({
                email: "crispy@gmail.com",
                password: "kigalfwer"
            })
            .end((err, res) => {
                res.should.have.status(401);
                res.should.be.an('object');
                done();
            });
    });

});


describe("POST /api/v1/auth/register", () => {
    after(async () => {
        await db.User.truncate();
    })

    it("should not be able to register if the name is missing", (done) => {
        chai.request(app)
            .post('/api/v1/auth/register')
            .send({
                email: faker.internet.email(),
                password: faker.name.firstName()
            })
            .end((err, res) => {
                res.should.have.status(400);
                res.should.be.an('object');
                done();
            });
    });

    it("should not be able to register if the email is missing", (done) => {
        chai.request(app)
            .post('/api/v1/auth/register')
            .send({
                name: faker.internet.email(),
                password: faker.name.firstName()
            })
            .end((err, res) => {
                res.should.have.status(400);
                res.should.be.an('object');
                done();
            });
    });

    it("should not be able to register if the password is incorrect or missing", (done) => {
        chai.request(app)
            .post('/api/v1/auth/register')
            .send({
                email: faker.internet.email(),
                name: faker.name.firstName()
            })
            .end((err, res) => {
                res.should.have.status(400);
                res.should.be.an('object');
                done();
            });
    });

    it("should not be able to register if the body contains all required fields", (done) => {
        const pass = faker.finance.iban().substr(0, 15);

        chai.request(app)
            .post('/api/v1/auth/register')
            .send({
                email: faker.internet.email(),
                name: faker.name.firstName(),
                password: pass,
                confirmPassword: pass
            })
            .end((err, res) => {
                res.should.have.status(201);
                res.should.be.an('object');
                done();
            });
    });
})