var expect = require('chai').expect;
import { User } from  './user';
import {MongoClient} from 'mongodb';

describe('User Model', () => {
    beforeEach(function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            db.collection('users', function (err, collection) {
                collection.remove({}, function (err, removed) {
                    done();
                });
            });
        });
    });

    it('should return null on a missing Facebook user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User.getFacebookUser(db, 'missing')
                .then(function (user) {
                    expect(user).to.be.a('null');
                    done();
                });
        });
    });

    it('should insert a Facebook user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User
                .upsertFacebookUser(db, {facebookId: 'test', name: 'test', email: 'test'})
                .then(function () {
                    User
                        .getFacebookUser(db, 'test')
                        .then((user) => {
                            expect(user).to.not.be.a('null');
                            expect(user.name).to.equal('test');
                            done();
                        });
                });
        });
    });

    it('should upsert a Facebook user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User.upsertFacebookUser(db, {facebookId: 'test', name: 'test', email: 'test'}).then(function () {
                User.getFacebookUser(db, 'test').then((user) => {
                    expect(user).to.not.be.a('null');
                    expect(user.name).to.equal('test');
                    expect(user.email).to.equal('test');
                    User.upsertFacebookUser(db, {facebookId: 'test', name: 'test2'}).then(function () {
                        User.getFacebookUser(db, 'test').then((user) => {
                            expect(user).to.not.be.a('null');
                            expect(user).to.have.property('name');
                            expect(user.name).to.equal('test2');
                            expect(user).to.not.have.property('email');
                            done();
                        });
                    });
                });
            });
        });
    });

    it('should return null on a missing Twitter user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User.getTwitterUser(db, 'missing')
                .then(function (user) {
                    expect(user).to.be.a('null');
                    done();
                });
        });
    });

    it('should insert a Twitter user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User
                .upsertTwitterUser(db, {twitterId: 'test', name: 'test', email: 'test'})
                .then(function () {
                    User
                        .getTwitterUser(db, 'test')
                        .then((user) => {
                            expect(user).to.not.be.a('null');
                            expect(user.name).to.equal('test');
                            done();
                        });
                });
        });
    });

    it('should upsert a Twitter user', function (done) {
        MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');
            User.upsertTwitterUser(db, {twitterId: 'test', name: 'test', email: 'test'}).then(function () {
                User.getTwitterUser(db, 'test').then((user) => {
                    expect(user).to.not.be.a('null');
                    expect(user.name).to.equal('test');
                    expect(user.email).to.equal('test');
                    User.upsertTwitterUser(db, {twitterId: 'test', name: 'test2'}).then(function () {
                        User.getTwitterUser(db, 'test').then((user) => {
                            expect(user).to.not.be.a('null');
                            expect(user).to.have.property('name');
                            expect(user.name).to.equal('test2');
                            expect(user).to.not.have.property('email');
                            done();
                        });
                    });
                });
            });
        });
    });
});