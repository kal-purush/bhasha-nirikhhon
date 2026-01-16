const config = {
  url: 'mongodb://localhost:27017/Users',
  options: {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    autoIndex: false,
  },
};

module.exports = config;