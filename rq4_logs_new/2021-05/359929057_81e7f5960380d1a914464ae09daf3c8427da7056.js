const swaggerJSDoc = require('swagger-jsdoc');

const swaggerDefinition = {
    info: {
        title: 'Test API',
        version: '1.0.0',
        description: 'API for testing purposes'
    },
    host: '0.0.0.0:8080',
    basePath: '/'
};

const options = {
    swaggerDefinition,
    apis: [
        './app/routers/test.router.js'
    ]
}

module.exports = swaggerJSDoc(options);