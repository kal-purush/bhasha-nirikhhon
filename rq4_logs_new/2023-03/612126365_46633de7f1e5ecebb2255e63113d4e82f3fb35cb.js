require('dotenv').config();
const PORT = process.env.PORT || 3000;

const swaggerOptions = {
  info: {
    version: '1.0.0',
    title: 'sendinGo',
    description: 'Sendingo',
  },
  servers: [
    {
      url: `/api`,
      description: 'BaseURL',
    },
  ],
  security: {
    // BasicAuth: {
    //   type: 'http',
    //   scheme: 'basic',
    // },
    Authorization: {
      type: 'http',
      scheme: 'bearer',
      bearerFormat: 'JWT',
      value: 'Bearer <JWT token here>',
    },
  },
  // Base directory which we use to locate your JSDOC files
  baseDir: __dirname,
  // Glob pattern to find your jsdoc files (multiple patterns can be added in an array)
  filesPattern: '../**/*.js',
  // URL where SwaggerUI will be rendered
  swaggerUIPath: '/api-docs',
  // Expose OpenAPI UI
  exposeSwaggerUI: true,
  // Set non-required fields as nullable by default
  notRequiredAsNullable: false,
  // You can customize your UI options.
  // you can extend swagger-ui-express config. You can checkout an example of this
  // in the `example/configuration/swaggerOptions.js`
  swaggerUiOptions: {
    swaggerOptions: {
      urls: [
        {
          url: `https://dev.sendingo-be.store/api`,
          name: 'Production',
        },
      ],
    },
  },
  // multiple option in case you want more that one instance
  multiple: true,
};

module.exports = swaggerOptions;