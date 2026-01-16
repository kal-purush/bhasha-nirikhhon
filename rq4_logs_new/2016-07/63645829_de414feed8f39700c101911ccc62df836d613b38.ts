///<reference path='../typings/tsd.d.ts'/>
import * as http from 'http';
import { expect } from 'chai';
import * as sinon from 'sinon';
import * as request from 'supertest';
import { ActionContext, Authenticator, Database, Dao, DaoOptions, Cache, Dispatcher } from 'nova-base';
import { createApp } from './../index';
import { Application, AppConfig } from '../lib/Application';
import { RouteController, RouteConfig, EndpointConfig, ViewBuilder } from '../lib/RouteController';
import { MockDao } from './mocks/Database';

interface AdapterFunc {
    (this: ActionContext, inputs: any, authInfo: any): Promise<any>;
}
interface ActionFunc {
    (this: ActionContext, inputs: any): Promise<any>;
}

let server: http.Server;
let router: RouteController;
let app: Application;
let authenticator: Authenticator;
let dao: Dao;
let database: Database;
let cache: Cache;
let dispatcher: Dispatcher;
let appOptions: AppConfig;

let adapter: AdapterFunc;
let action: ActionFunc;
let view: ViewBuilder<any>;
let routeConfig: RouteConfig;
let endpointConfig: EndpointConfig<any, any>;

const authResult: any = { token: '1234567890' };
const daoOptions: DaoOptions = { startTransaction: true };

describe('NOVA-SERVER -> tests;', () => {
    beforeEach(() => {
        server = http.createServer();
        router = new RouteController();

        authenticator = sinon.stub().returns(Promise.resolve(authResult));
        dao = new MockDao(daoOptions);
        database = { connect: sinon.stub().returns(Promise.resolve(dao)) };
        dispatcher = { dispatch: sinon.stub().returns(Promise.resolve()) };

        cache = {
            get    : sinon.stub().returns(Promise.resolve()),
            set    : sinon.stub(),
            execute: sinon.stub().returns(Promise.resolve()),
            clear  : sinon.stub()
        };

        sinon.spy(dao, 'release');

        appOptions = {
            name         : 'Test API Server',
            version      : '0.0.1',
            webServer    : server,
            ioServer     : undefined,
            database     : database,
            cache        : cache,
            dispatcher   : dispatcher,
            authenticator: authenticator,
            settings     : {}
        };
    });

    describe('should create app without errors;', () => {
        beforeEach(() => {
            app = createApp(appOptions);
        });

        it('app object should be instance of Application', () => {
            expect(app).to.be.instanceof(Application);
        });

    });

    // describe('should run app server without any attached routes;', () => {
    //     it('should return 404', done => {
    //         app = createApp(appOptions);
    //         try {
    //             request(app.webServer)
    //                 .get('/')
    //                 .expect(404, done);
    //
    //         } catch (err) {
    //             done(err);
    //         }
    //
    //     });
    // });

    // describe('should return error if no actions was provided;', () => {
    //     it('should return error', done => {
    //         try {
    //             app = createApp(appOptions);
    //             router = new RouteController();
    //             routeConfig = { get: undefined };
    //             router.set('/', routeConfig);
    //
    //             app.register('/', router);
    //
    //             app = createApp(appOptions);
    //             done('errror')
    //         } catch (err) {
    //             done(err);
    //         }
    //     });
    // });

    describe('GET \'\\\' route;', () => {
        beforeEach(() => {
            app = createApp(appOptions);

            router = new RouteController();

            action = sinon.stub().returns(Promise.resolve('result'));
            view = sinon.stub().returns(Promise.resolve('result'));

            endpointConfig = { action, response: view };
            routeConfig = { get: endpointConfig };

            router.set('/', routeConfig);

            app.register('/', router);
        });

        it('should return 200', done => {
            request(app.webServer)
                .get('/')
                .query({ author: 'test' })
                .expect(200)
                .end((err, res) => {
                    if (err) {
                        return done(err);
                    }

                    done();
                });
        });
    });
});