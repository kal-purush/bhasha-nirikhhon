// # Ghost Configuration
// Setup your Ghost install for various environments
// Documentation can be found at http://support.ghost.org/config/

var path = require('path'),
    config;

config = {
    // ### Testing MySQL
    // Used by Travis - Automated testing run through GitHub
    'production': {
        url: 'http://www.guizhi.me',
        database: {
            client: 'sqlite3',
            connection: {
                filename: path.join(__dirname, '/content/data/ghost.db')
            },
            debug: false
        },
        server: {
            host: '127.0.0.1',
            port: '2368'
        },
        logging: false
    },
};

// Export config
module.exports = config;