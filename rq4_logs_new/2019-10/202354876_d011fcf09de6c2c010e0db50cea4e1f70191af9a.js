import swaggerJSDoc from 'swagger-jsdoc';

const swaggerDefinition = {
  info: {
    title: 'Meetapp API',
    version: '1.0.0',
    description: 'Documentation for the Meetapp API',
  },
  securityDefinitions: {
    bearerAuth: {
      type: 'apiKey',
      name: 'Authorization',
      scheme: 'bearer',
      in: 'header',
    },
  },
};

const swaggerOptions = {
  swaggerDefinition,
  apis: ['*/**/routes.js', '*/**/models/*.js'],
};

export default swaggerJSDoc(swaggerOptions);