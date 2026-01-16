///<reference path='../typings/tsd.d.ts'/>
import { expect } from 'chai';
import * as sinon from 'sinon';
import * as io from 'socket.io-client';
import { Authenticator, Database, Dao, DaoOptions, validate } from 'nova-base';
import { createApp } from './../index';
import { Application, AppConfig } from '../lib/Application';
import { SocketListener, HandlerConfig } from '../lib/SocketListener';
import { MockDao } from './mocks/Database';

let app: Application;
let listener: SocketListener;
let authenticator: Authenticator;
let dao: Dao;
let database: Database;

let socketClient: any;
let appConfig: AppConfig;
let listenerConfig: HandlerConfig<any, any>;

const daoOptions: DaoOptions = { startTransaction: true };
const authResult: any = { results: 'auth results' };
const actionResult: any = { results: 'action results' };
const adapterResult: any = { results: 'adapter results' };
const authOptions: any = { isRequired: false };
const listenerDaoOptions: DaoOptions = { startTransaction: true };
const authToken: string = 'testAuthToken';
const testPort: number = 3000;

const payload: any = {
    id    : '12345',
    query : {
        a: 'a',
        b: 'b'
    },
    params: [1, 2, 3]
};

describe('NOVA-SERVER -> SocketListener;', () => {
    beforeEach(() => {
        dao = new MockDao(daoOptions);
        database = { connect: sinon.stub().returns(Promise.resolve(dao)) };
        authenticator = sinon.stub().returns(Promise.resolve(authResult));
        authenticator.toOwner = sinon.stub().returns(authToken);
    });

    describe('should create socket listener and connect client;', () => {
        beforeEach(done => {
            appConfig = {
                name         : 'Test API Server',
                version      : '0.0.1',
                database     : database,
                authenticator: authenticator
            };

            app = createApp(appConfig);
            listener = new SocketListener();
            listenerConfig = {
                action : sinon.stub().returns(Promise.resolve(actionResult)),
                adapter: sinon.stub().returns(Promise.resolve(adapterResult))
            };
            listener.on('test', listenerConfig);
            app.register('/', listener);
            app.on('error', () => undefined);

            (app.webServer as any).listen(testPort);

            socketClient = io(`http://localhost:${testPort}`, {
                query: { authorization: `token ${authToken}` }
            });

            socketClient.on('connect', done);
            socketClient.on('error', done);
        });

        afterEach(done => {
            socketClient.off('error');
            app.webServer.close();
            done();
        });

        it('should connect socket client', () => {
            expect(socketClient.connected).to.be.true;
        });

        it('authenticator should be called once', () => {
            expect((authenticator as any).calledOnce).to.be.true;
        });

        it('authenticator should be called with right arguments', () => {
            expect((authenticator as any).firstCall.calledWithExactly({
                scheme     : 'token',
                credentials: authToken
            }, undefined)).to.be.true;
        });

        it('authenticator.toOwner should be called once', () => {
            expect((authenticator.toOwner as any).calledOnce).to.be.true;
        });

        it('authenticator.toOwner should be called with right arguments', () => {
            expect((authenticator.toOwner as any).firstCall.calledWithExactly(authResult)).to.be.true;
        });

        it('action should not be called', () => {
            expect((listenerConfig.action as any).called).to.be.false;
        });

        it('adapter should not be called', () => {
            expect((listenerConfig.adapter as any).called).to.be.false;
        });
    });

    describe('should should receive client message and call all functions with right arguments;', () => {
        beforeEach(() => {
            appConfig = {
                name         : 'Test API Server',
                version      : '0.0.1',
                database     : database,
                authenticator: authenticator
            };

            app = createApp(appConfig);
            app.on('error', () => undefined);
        });

        describe('when payload was provided;', () => {
            beforeEach(done => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult))
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });

                socketClient.on('error', done);
                socketClient.on('connect', () => {
                    socketClient.emit('test', payload, err => {
                        if (err) {
                            return done(err);
                        }

                        done();
                    });
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('authenticator should be called twice', () => {
                expect((authenticator as any).calledTwice).to.be.true;
            });

            it('authenticator should be always called with right arguments', () => {
                expect((authenticator as any).alwaysCalledWithExactly({
                    scheme     : 'token',
                    credentials: authToken
                }, undefined)).to.be.true;
            });

            it('authenticator.toOwner should be called once', () => {
                expect((authenticator.toOwner as any).calledOnce).to.be.true;
            });

            it('authenticator.toOwner should be called with right arguments', () => {
                expect((authenticator.toOwner as any).firstCall.calledWithExactly(authResult)).to.be.true;
            });

            it('adapter should be called once', () => {
                expect((listenerConfig.adapter as any).calledOnce).to.be.true;
            });

            it('adapter should be called with right arguments', () => {
                expect((listenerConfig.adapter as any).firstCall.calledWithExactly(payload, authResult)).to.be.true;
            });

            it('action should be called once', () => {
                expect((listenerConfig.action as any).calledOnce).to.be.true;
            });

            it('action should be called with right arguments', () => {
                expect((listenerConfig.action as any).firstCall.calledWithExactly(adapterResult)).to.be.true;
            });
        });

        describe('when payload was not provided;', () => {
            beforeEach(done => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult))
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });

                socketClient.on('error', done);
                socketClient.on('connect', () => {
                    socketClient.emit('test', {}, err => {
                        if (err) {
                            return done(err);
                        }

                        done();
                    });
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('authenticator should be called twice', () => {
                expect((authenticator as any).calledTwice).to.be.true;
            });

            it('authenticator should be always called with right arguments', () => {
                expect((authenticator as any).alwaysCalledWithExactly({
                    scheme     : 'token',
                    credentials: authToken
                }, undefined)).to.be.true;
            });

            it('authenticator.toOwner should be called once', () => {
                expect((authenticator.toOwner as any).calledOnce).to.be.true;
            });

            it('authenticator.toOwner should be called with right arguments', () => {
                expect((authenticator.toOwner as any).firstCall.calledWithExactly(authResult)).to.be.true;
            });

            it('adapter should be called once', () => {
                expect((listenerConfig.adapter as any).calledOnce).to.be.true;
            });

            it('adapter should be called with right arguments', () => {
                expect((listenerConfig.adapter as any).firstCall.calledWithExactly({}, authResult)).to.be.true;
            });

            it('action should be called once', () => {
                expect((listenerConfig.action as any).calledOnce).to.be.true;
            });

            it('action should be called with right arguments', () => {
                expect((listenerConfig.action as any).firstCall.calledWithExactly(adapterResult)).to.be.true;
            });
        });

        describe('when auth options was provided in listener config;', () => {
            beforeEach(done => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult)),
                    auth   : authOptions
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });

                socketClient.on('error', done);
                socketClient.on('connect', () => {
                    socketClient.emit('test', {}, err => {
                        if (err) {
                            return done(err);
                        }

                        done();
                    });
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('authenticator should be called twice', () => {
                expect((authenticator as any).calledTwice).to.be.true;
            });

            it('authenticator should be always called with right arguments first time', () => {
                expect((authenticator as any).firstCall.calledWithExactly({
                    scheme     : 'token',
                    credentials: authToken
                }, undefined)).to.be.true;
            });

            it('authenticator should be always called with right arguments second time', () => {
                expect((authenticator as any).secondCall.calledWithExactly({
                    scheme     : 'token',
                    credentials: authToken
                }, authOptions)).to.be.true;
            });

            it('authenticator.toOwner should be called once', () => {
                expect((authenticator.toOwner as any).calledOnce).to.be.true;
            });

            it('authenticator.toOwner should be called with right arguments', () => {
                expect((authenticator.toOwner as any).firstCall.calledWithExactly(authResult)).to.be.true;
            });

            it('adapter should be called with right arguments', () => {
                expect((listenerConfig.adapter as any).firstCall.calledWithExactly({}, authResult)).to.be.true;
            });

            it('action should be called with right arguments', () => {
                expect((listenerConfig.action as any).firstCall.calledWithExactly(adapterResult)).to.be.true;
            });
        });

        describe('when dao options was not provided in listener config;', () => {
            beforeEach(done => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult)),
                    auth   : authOptions
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });

                socketClient.on('error', done);
                socketClient.on('connect', () => {
                    socketClient.emit('test', {}, err => {
                        if (err) {
                            return done(err);
                        }

                        done();
                    });
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('database.connect should be called twice', () => {
                expect((appConfig.database.connect as any).calledTwice).to.be.true;
            });

            it('database.connect should be always called with right arguments', () => {
                expect((appConfig.database.connect as any).alwaysCalledWithExactly({ startTransaction: false })).to.be.true;
            });
        });

        describe('when dao options was provided in listener config;', () => {
            beforeEach(done => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult)),
                    auth   : authOptions,
                    dao    : listenerDaoOptions
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });

                socketClient.on('error', done);
                socketClient.on('connect', () => {
                    socketClient.emit('test', {}, err => {
                        if (err) {
                            return done(err);
                        }

                        done();
                    });
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('database.connect should be called twice', () => {
                expect((appConfig.database.connect as any).calledTwice).to.be.true;
            });

            it('database.connect should be always called with right arguments first time', () => {
                expect((appConfig.database.connect as any).firstCall.calledWithExactly({ startTransaction: false })).to.be.true;
            });

            it('database.connect should be always called with right arguments second time', () => {
                expect((appConfig.database.connect as any).secondCall.calledWithExactly(listenerDaoOptions)).to.be.true;
            });
        });
    });

    describe('should emit error', () => {
        beforeEach(() => {
            appConfig = {
                name         : 'Test API Server',
                version      : '0.0.1',
                database     : database,
                authenticator: authenticator
            };

            app = createApp(appConfig);
            app.on('error', () => undefined);
        });

        describe('if authorization data was not provided;', () => {
            beforeEach(() => {
                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult))
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`);
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('socket client should return error', done => {
                socketClient.on('error', err => {
                    try {
                        expect(err).to.equal('Authorization header was not provided');
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });

            it('app should emmit error with \'authorization header was not provided\' message', done => {
                app.on('error', err => {
                    try {
                        expect(err.message).to.equal('Authorization header was not provided');
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });

            it('app should emmit error with 401 status', done => {
                app.on('error', err => {
                    try {
                        expect(err.status).to.equal(401);
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });
        });

        describe('if authenticator was rejected;', () => {
            beforeEach(() => {
                authenticator = function () {
                    try {
                        validate.authorized(false, 'Invalid token');
                    } catch (e) {
                        return Promise.reject(e);
                    }
                };

                authenticator.toOwner = sinon.stub().returns(authToken);

                appConfig.authenticator = authenticator;

                app = createApp(appConfig);
                app.on('error', () => undefined);

                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult))
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('socket client should return error', done => {
                socketClient.on('error', err => {
                    try {
                        expect(err).to.equal('Failed to execute authenticateSocket action: Invalid token');
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });

            it('app should emmit error with \'Invalid token\' message', done => {
                app.on('error', err => {
                    try {
                        expect(err.message).to.match(/Invalid token/);
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });

            it('app should emmit error with 401 status', done => {
                app.on('error', err => {
                    try {
                        expect(err.status).to.equal(401);
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });
        });

        describe('if authenticator return error;', () => {
            beforeEach(() => {
                authenticator = sinon.stub().throws('TypeError');
                authenticator.toOwner = sinon.stub().returns(authToken);

                appConfig.authenticator = authenticator;

                app = createApp(appConfig);
                app.on('error', () => undefined);

                listener = new SocketListener();
                listenerConfig = {
                    action : sinon.stub().returns(Promise.resolve(actionResult)),
                    adapter: sinon.stub().returns(Promise.resolve(adapterResult))
                };
                listener.on('test', listenerConfig);
                app.register('/', listener);

                (app.webServer as any).listen(testPort);

                socketClient = io(`http://localhost:${testPort}`, {
                    query: { authorization: `token ${authToken}` }
                });
            });

            afterEach(done => {
                socketClient.off('error');
                app.webServer.close();
                done();
            });

            it('socket client should return error', done => {
                socketClient.on('error', err => {
                    try {
                        expect(err).to.match(/Failed to execute/);
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });

            it('app should emmit error with \'Failed to execute\' message', done => {
                app.on('error', err => {
                    try {
                        expect(err.message).to.match(/Failed to execute/);
                        done();
                    } catch (err) {
                        done(err);
                    }
                });
            });
        });
    });
});