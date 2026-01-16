import request from 'supertest';

import App from './../../src/app';

export default (): void => {
    let server = null;
    let app = null;

    before(async () => {
        server = new App();
        app = server.app;

        await server.listen();
    });

    after(() => {
        server.close();
    });

    it('should return 401 if no token was provided', (done) => {
        request(app)
            .get('/recipe/all')
            .expect(401, {
                message: "'X-API-KEY' header required",
                errors: [
                    {
                        path: '/recipe/all',
                        message: "'X-API-KEY' header required"
                    }
                ]
            }, done);
    });

    it('should return 401 if an invalid token was provided', (done) => {
        request(app)
            .get('/recipe/all')
            .set('X-API-KEY', 'INVALID_TOKEN')
            .expect(401, {
                message: 'API key is missing or invalid.',
                errors: [
                    {
                        path: '/recipe/all',
                        message: 'API key is missing or invalid.'
                    }
                ]
            }, done);
    });

    it('should return 200 if a valid token was provided', (done) => {
        request(app)
            .get('/recipe/all')
            .set('X-API-KEY', 'admin')
            .expect(200, done);
    });
};