const token = require('./tokenEndpoint.js');
const threat = require('./threatHandler.js');

const debug = require('debug')('oauth:config');
debug.log = console.log.bind(console);

class OAuth {
    constructor(config) {
        this.config = {
            enableLogThreat: true,
            onThreat: null,
            secret: null,
            refreshSecret: null,
            issuer: null,
            ...config
        };

        debug({
            ...(({ ...conf }) => {
                delete conf.secret;
                delete conf.refreshSecret;

                if (conf.onThreat)
                    conf.onThreat = true;

                return conf;
            })(this.config)
        });
        if (!this.config.secret)
            throw new Error('oauth configuration secret is not set');
        if (!this.config.refreshSecret)
            throw new Error('oauth configuration refresh token secret is not set');
        if (!this.config.issuer)
            throw new Error('oauth configuration issuer is not set');

        this.supportedGrantTypes = ['client_credentials', 'password', "refresh_token"];
    }

    get onThreat() {
        return threat(this);
    }

    get tokenEndpoint() {
        return [token(this), (err, req, res, next) => {
            throw err;
        }];
    }
}

module.exports = OAuth;
