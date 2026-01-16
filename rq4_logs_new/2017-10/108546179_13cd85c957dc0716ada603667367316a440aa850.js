require('dotenv').config({path: './server/tests/test.env'});

const chai = require('chai');
const should = chai.should();
const chaiHttp = require('chai-http');
chai.use(chaiHttp);

const server = require('../../bin/www');

const { Product, Ingredient, Category, Product_Ingredient, User } = require('../models');

const user = 'Mocha';
const pwd = 'chachachaMO';
const name = 'Mocha Tester';
const email = 'tester@mocha.me';

var token;

describe('Routes: ', () => {
    before( (done) => {

        Product_Ingredient.destroy({where: {}});
        Product.destroy({where: {}});
        Ingredient.destroy({where: {}});
        Category.destroy({where: {}});
        User.destroy({where: {}});
        done();

        // this.setTimeout(0);
        // return sequelize.sync({force:true, logging: console.log})
        // .then( ()=> { return Product_Ingredient.destroy({where: {}}); })
        // .then( ()=> { return Product.destroy({where: {}}); } )
        // .then( ()=> { return Ingredient.destroy({where: {}}); } )
        // .then( ()=> { return Category.destroy({where: {}});} )
        // .then( ()=> { return User.destroy({where: {}}); })
        // .then( ()=> setTimeout(done, 0))
        // .catch( err => console.log("Something") )
    })

    describe('POST /user/register', () => {
        it('should not accept invalid username input', (done) => {
            chai.request(server)
                .post('/user/register')
                .send({
                    username: 'Mocha 23',
                    password: 'chachachaMO',
                    name: 'Mocha Tester',
                    email: 'tester@mocha.me'
                })
                .end( (err, res) => {
                    should.exist(err);
                    res.status.should.eql(400);
                    res.should.be.json;
                    res.body.success.should.eql(false);
                    done();
                })
        })

        it('should not accept invalid password input', (done) => {
            chai.request(server)
                .post('/user/register')
                .send({
                    username: 'Mocha',
                    password: '{$or: {$gt:1}}',
                    name: 'Mocha Tester',
                    email: 'tester@mocha.me'
                })
                .end( (err, res) => {
                    should.exist(err);
                    res.status.should.eql(400);
                    res.should.be.json;
                    res.body.success.should.eql(false);
                    done();
                })
        })

        it('should not accept invalid name input', (done) => {
            chai.request(server)
                .post('/user/register')
                .send({
                    username: 'Mocha',
                    password: 'chahcha',
                    name: 'Mocha Tester (M@D D0LLA $!GN)',
                    email: 'tester@mocha.me'
                })
                .end( (err, res) => {
                    should.exist(err);
                    res.status.should.eql(400);
                    res.should.be.json;
                    res.body.success.should.eql(false);
                    done();
                })
        })

        it('should not accept invalid email input', (done) => {
            chai.request(server)
                .post('/user/register')
                .send({
                    username: 'Mocha',
                    password: 'chahcha',
                    name: 'Mocha Tester',
                    email: 'come@me@your.peril'
                })
                .end( (err, res) => {
                    should.exist(err);
                    res.status.should.eql(400);
                    res.should.be.json;
                    res.body.success.should.eql(false);
                    done();
                })
        })

        // it('should register user with valid information', (done) => {
        //     chai.request(server)
        //         .post('/user/register')
        //         .send({
        //             username: user,
        //             password: pwd,
        //             name: name,
        //             email: email
        //         })
        //         .end( (err, res) => {
        //             should.not.exist(err);
        //             res.status.should.eql(201);
        //             res.should.be.json;
        //             res.body.success.should.eql(true);
        //             // res.body.data.username.should.eql(user);
        //             // res.body.data.name.should.eql(name);
        //             // res.body.data.email.should.eql(email);
        //             // should.not.exist(res.body.data.password);
        //             done();
        //         })
        // })

        // it('should not register user with same information', (done) => {
        //     chai.request(server)
        //         .post('/user/register')
        //         .send({
        //             username: user,
        //             password: pwd,
        //             name: name,
        //             email: email
        //         })
        //         .end( (err, res) => {
        //             should.exist(err);
        //             res.status.should.eql(400);
        //             res.should.be.json;
        //             res.body.success.should.eql(false);
        //             done();
        //         })
        // })
    })



    describe('GET /category', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .get('/api/category')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('POST /category', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .post('/api/category')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('GET /category/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .get('/api/category/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('PUT /category/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .put('/api/category/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('DELETE /category/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .delete('/api/category/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('GET /ingredient', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .get('/api/ingredient')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('POST /ingredient', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .post('/api/ingredient')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('GET /ingredient/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .get('/api/ingredient/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('PUT /ingredient/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .put('/api/ingredient/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    describe('DELETE /ingredient/:id', () => {
        it('should return failure - no auth', (done) => {
            chai.request(server)
            .delete('/api/ingredient/1')
            .end( (err, res) => {
                should.exist(err);
                res.status.should.eql(401);
                res.should.be.json;
                res.body.success.should.eql(false);
                done();
            })
        })

        
    })

    after( () => {
        process.exit(0);
    })
})