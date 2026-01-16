import swaggerJsdoc from 'swagger-jsdoc';

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'API Documentation',
      version: '1.0.0',
      description:
        'Documentation for the API endpoints and their expected inputs/outputs.',
    },
    components: {
      schemas: {
        User: {
          type: 'object',
          properties: {
            _id: {
              type: 'string',
              description: 'The unique identifier for the user.',
              example: '8DKZvZ9Wv9Hk9WEzGbQN',
            },
            email: {
              type: 'string',
              description: "The user's email address.",
              example: 'gabriella90631@gmail.com',
            },
            password: {
              type: 'string',
              description: "The user's hashed password.",
              example:
                '$2b$10$Uo5t0qVFHOTa1d13e4SD6eELcnVJfCGv5amK5SM3zB/QPt5H28U9W',
            },
            sub: {
              type: 'string',
              description: "The user's subject identifier.",
              example: '$2b$10$jfc.GIURY',
            },
            username: {
              type: 'string',
              description: "The user's chosen username.",
              example: 'Gabriella',
            },
            graduated: {
              type: 'boolean',
              description: 'Indicates if the user has graduated.',
              example: false,
            },
            overallScore: {
              type: 'integer',
              description: "The user's cumulative score.",
              example: 410,
            },
          },
          required: [
            '_id',
            'email',
            'password',
            'sub',
            'username',
            'graduated',
            'overallScore',
          ],
        },

        Email: {
          type: 'object',
          properties: {
            _id: {
              type: 'string',
              description: 'Unique identifier for the email object',
            },
            message: {
              type: 'string',
              description: 'Full email content, including subject and body',
            },
            scam: {
              type: 'boolean',
              description: 'Indicates if the email is flagged as a scam',
            },
            type: {
              type: 'string',
              description: 'Type of the message (e.g., email, sms)',
            },
            category: {
              type: 'string',
              description:
                'Category associated with the email (e.g., online_shopping, social)',
            },
            feedback: {
              type: 'string',
              description:
                'Feedback about the email, if any (e.g., reason for scam flag)',
            },
            emailSender: {
              type: 'string',
              description: 'Email address of the sender',
            },
          },
        },

        Text: {
          type: 'object',
          properties: {
            _id: {
              type: 'string',
              description: 'Unique identifier for the email object',
            },
            message: {
              type: 'string',
              description: 'Full message content, including subject and body',
            },
            scam: {
              type: 'boolean',
              description: 'Indicates if the message is flagged as a scam',
            },
            type: {
              type: 'string',
              description: 'Type of the message (e.g., email, sms)',
            },
            category: {
              type: 'string',
              description:
                'Category associated with the message (e.g., online_shopping, social)',
            },
            feedback: {
              type: 'string',
              description:
                'Feedback about the email, if any (e.g., reason for scam flag)',
            },
            phoneNumber: {
              type: 'string',
              description: 'Email address of the sender',
            },
          },
        },

        ResourceDocument: {
          type: 'object',
          properties: {
            _id: {
              type: 'string',
              description: 'Unique identifier for the resource',
            },
            category: {
              type: 'string',
              description:
                'Category of the scam resource (e.g., Phishing, Malware)',
            },
            content: {
              type: 'string',
              description: 'Description of the scam and its techniques',
            },
            links: {
              type: 'array',
              items: {
                type: 'string',
                format: 'uri',
              },
              description:
                'Array of URLs containing relevant information on the scam',
            },
            image: {
              type: 'string',
              description: 'Base64 encoded image data (optional)',
            },
          },
        },
      },
    },
  },
  apis: ['./src/**/*.ts'], // Path to the API docs
};

const specs = swaggerJsdoc(options);
export default specs;