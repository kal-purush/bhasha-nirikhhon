import { flatten } from 'lodash';

// setup express & express.Router mock
jest.mock('express', () => ({
    Router: jest.fn(),
}));
import express from 'express';
import { Request, Response } from 'jest-express';
const router = {
    get: jest.fn(),
    use: jest.fn(),
};
express.Router.mockImplementation(() => router);

// setup passport mock
jest.mock('passport', () => ({
    authenticate: jest.fn(),
}));
import passport from 'passport';
const authenticateObject = new Object();
passport.authenticate.mockImplementation(() => authenticateObject);

// use require to ensure synchronity
const userRouter = require('@/routes/api/user').default;

function getRouterRoutes(httpMethod) {
    const httpMethodCalls = router[httpMethod].mock.calls;
    return httpMethodCalls.reduce((acc, call) => {
        const [route, handler] = call;
        acc[route] = handler;
        return acc;
    }, {});
}

describe('User api route', () => {
    const routeMap = {
        get: getRouterRoutes('get'),
    };

    it('returns Router', () => {
        expect(userRouter).toBe(express.Router());
    });

    describe('auth middlewares', () => {
        const middlewares = flatten(router.use.mock.calls);

        describe('first middleware', () => {
            it('is passport jwt authenticate middleware', () => {
                expect(passport.authenticate).toHaveBeenCalled();
                expect(middlewares[0]).toBe(authenticateObject);
            });
            it('has been called with "jwt" strategy, with session disabled', () => {
                expect(passport.authenticate).toHaveBeenCalledWith('jwt', { session: false });
            });
        });

        describe('second middleware', () => {
            const mw = middlewares[1];

            it('calls next if req.user exists (user is authenticated)', () => {
                const req = new Request('/');
                req.user = {};
                const res = {};
                const next = jest.fn();
                mw(req, res, next);
                expect(next).toHaveBeenCalledTimes(1);
            });

            it('calls res.sendStatus() with 401 if req.user not exists (user not authenticated)', () => {
                const req = new Request('/');
                const res = new Response();
                const next = jest.fn();
                mw(req, res, next);
                expect(next).toHaveBeenCalledTimes(0);
                expect(res.sendStatus).toHaveBeenCalledTimes(1);
            });
        });
    });

    describe('GET /', () => {
        it('exists', () => {
            expect(routeMap.get).toHaveProperty('/', expect.any(Function));
        });

        it('calls res.status(200).json() with the value of req.user.toApiObject()', () => {
            const rootHandler = routeMap.get['/'];
            const apiObject = new Object();
            const req = new Request('/');
            req.user = {
                toApiObject: jest.fn(() => apiObject),
            };
            const res = new Response();
            rootHandler(req, res);
            expect(res.status).toHaveBeenCalledTimes(1);
            expect(res.status).toHaveBeenCalledWith(200);
            expect(res.json).toHaveBeenCalledTimes(1);
            expect(res.json).toHaveBeenCalledWith(apiObject);
        });
    });
});