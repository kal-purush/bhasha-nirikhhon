var config = {
  port: process.env.PORT || 3000,
  client: {
    mongodb: {
      database: 'express-server-videos',
      collection: 'videos'
    }
  }
};

module.exports = config;