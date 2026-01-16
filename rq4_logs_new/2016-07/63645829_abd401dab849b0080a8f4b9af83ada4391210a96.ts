///<reference path='../typings/tsd.d.ts'/>
import { expect } from 'chai';
import * as sinon from 'sinon';
import * as request from 'supertest';
import { Database, Dao } from 'nova-base';
import { createApp } from './../index';
import { Application, AppConfig } from '../lib/Application';
import { MockDao } from './mocks/Database';

let app: Application;
let dao: Dao;
let database: Database;

let appConfig: AppConfig;

let fakeRequest: any;

describe('NOVA-SERVER -> Application;', () => {
    beforeEach(() => {
        dao = new MockDao({ startTransaction: true });
        database = { connect: sinon.stub().returns(Promise.resolve(dao)) };
    });

    describe('should create app;', () => {
        beforeEach(() => {
            appConfig = {
                name    : 'Test API Server',
                version : '0.0.1',
                database: database
            };
        });

        it('app object should be instance of Application', () => {
            app = createApp(appConfig);

            expect(app).to.be.instanceof(Application);
        });

        it('should return error if database was not provided', done => {
            delete appConfig.database;

            try {
                app = createApp(appConfig);
                done('should return error');
            } catch (err) {
                expect(err).to.match(/Database is undefined/);
                done();
            }
        });

        it('should return error if server name was not provided', done => {
            delete appConfig.name;

            try {
                app = createApp(appConfig);
                done('should return error');
            } catch (err) {
                expect(err).to.match(/name is undefined/);
                done();
            }
        });

        it('should return error if server version was not provided', done => {
            delete appConfig.version;

            try {
                app = createApp(appConfig);
                done('should return error');
            } catch (err) {
                expect(err).to.match(/version is undefined/);
                done();
            }
        });
    });

    describe('should run app server without any attached routes;', () => {
        beforeEach(() => {
            appConfig = {
                name    : 'Test API Server',
                version : '0.0.1',
                database: database
            };

            app = createApp(appConfig);
            fakeRequest = request(app.webServer)
                .get('/');

            app.on('error', () => undefined);
        });

        it('should return 404', done => {
            fakeRequest
                .expect(404, done);
        });

        it('should emmit error with \'does not exist\' message', done => {
            fakeRequest.end(() => {
            });

            app.on('error', err => {
                try {
                    expect(err.message).to.equal('Endpoint for / does not exist');
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });

        it('should emmit error with 404 status', done => {
            fakeRequest.end(() => {
            });

            app.on('error', err => {
                try {
                    expect(err.status).to.equal(404);
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });
    });
});