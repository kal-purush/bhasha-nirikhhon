module.exports = ({ env }) => ({
    defaultConnection: 'default',
    connections: {
      default: {
        connector: 'bookshelf',
        settings: {
          client: 'sqlite',
          filename: env('DATABASE_FILENAME', '.tmp/data.db'), // Update the path as needed
        },
        options: {
          useNullAsDefault: true,
        },
      },
    },
  });
  