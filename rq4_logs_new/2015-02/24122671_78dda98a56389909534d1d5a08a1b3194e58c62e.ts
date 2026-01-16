/** Requires base64.js from base-64 package*/


app.factory('httpInterceptor', ['TokenService', 'TimestampAndNonceService', '$location', '$q', 'growl',
    function (TokenService, TimestampAndNonceService, $location, $q) {
        return {
            request: function (config) {
                // oauth headers
                if (TokenService.getToken()) {
                    config.headers.Authorization = 'session_token=' + TokenService.getToken();
                }

                //nonce and timestamp headers
                config.headers['X-Timestamp'] = TimestampAndNonceService.getTimestamp();
                config.headers['X-Nonce'] = TimestampAndNonceService.getNonce();

                //configure cross domain
                delete config['X-Requested-With'];
                config.crossDomain = true;

                // json headers
                config.headers.Accept = 'application/json';
                config.headers['Content-Type'] = 'application/json';

                return config;
            },
            responseError: function (rejection) {
                if (rejection.status === 401) {
                    TokenService.setToken(null);
                    $location.path('/login');
                }
                return $q.reject(rejection);
            }
        };
    }]);


