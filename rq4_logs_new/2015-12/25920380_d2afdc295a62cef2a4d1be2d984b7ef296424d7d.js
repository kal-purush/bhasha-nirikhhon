// (c) 2013-2015 Nive GmbH - nive.io
// 
// nive-angular v0.8.3
// 
// License: Released under MIT-License. See http://jquery.org/license
// Docs: http://www.nive.co/docs/webapi
//
angular.module('nive.resource', []);
angular.module('nive.services', ['nive.resource']);
angular.module('nive', ['nive.services']);

// (c) 2013-2015 Nive GmbH - nive.io
// This file is released under the MIT-License. See http://jquery.org/license
//
// Nive api endpoint url construction
// Docs: http://www.nive.co/docs/webapi/endpoint.html
//
// Requires <nothing>


window.nive = window.nive || {};
nive.endpoint = nive.endpoint || {};
(function () {
'use strict';


nive.endpoint.makeUrl = function (options, path) {
    /*
     options: method, service, domain, path, secure, version
     path: additional relative path to be used in services with tree like structures
    * */
    options = options||{};
    var defaultDomain = '.nive.io';
    var protocol = 'https';

    // protocol
    if(options.secure==false) { protocol = 'http:'; }
    else if(protocol.indexOf(':')!=protocol.length-1) { protocol += ':'; }

    // domain
    var domain = options.domain || '';
    if(domain) {
        // if '.' contained in domain, a fully qualified domain expected
        domain = domain.indexOf('.')==-1 ? domain+defaultDomain : domain;
    }

    // version
    var version = options.version || 'api';

    // method
    var method = options.method;

    // construct path
    if(path||options.path) {
        if(!path) {
            path = options.path;
        } else if(options.path&&!path.indexOf('/')==0) {
            path = options.path+path;
        }
        // relative directory
        // this option is not supported by all services
        if(path.indexOf('./')==0||path.indexOf('../')==0) {
            if(path.lastIndexOf('/')!=path.length-1) { path += '/'; }
            return path + method;
        }
        // remove slash
        if(path.indexOf('/')==0) { path = path.substr(1, path.length); }
        if(path.lastIndexOf('/')==path.length-1) { path = path.substr(0, path.length-1); }
    }

    // service name
    if(!options.service && !options.path && !path) { throw 'Invalid service name'; }
    var service = options.service||'';

    // make url
    var url = '';
    if(domain) { url = protocol + '//' + domain; }
    url += '/' + service;
    if(version) { url += '/'+version; }
    if(path) { url += '/'+path; }
    if(method) { url += '/'+method; }
    return url;
};

nive.endpoint.EndpointException = function (message) {
    this.message = message;
    this.name = 'EndpointException';
};

})();

(function () {
'use strict';

angular.module('nive.resource').provider('NiveAPI', function () {

    var _$q,
        successHandler = function (response) {
            return response.data;
        },
        errorHandler = function (response) {

            // if the request was not handled by the server (or what not handles properly - ex. server error),
            // return a general error message
            if (!angular.isObject(response.data) || !response.data.message) {
                return _$q.reject('NiveAPI: An unknown error occurred.');
            }

            // Otherwise, use expected error message.
            return _$q.reject(response.data.message);
        };

    this.successHandler = function (handler) {
        if (angular.isFunction(handler)) {
            successHandler = handler;
        }

        return successHandler;
    };

    this.errorHandler = function (handler) {
        if (angular.isFunction(handler)) {
            errorHandler = handler;
        }

        return errorHandler;
    };

    this.$get = function ($http, $q) {

        _$q = $q;

        return {
            get: function (service, remoteMethod, params, apiOptions) {
                return this.call(service, remoteMethod, params, 'GET', apiOptions);
            },

            post: function (service, remoteMethod, params, apiOptions) {
                return this.call(service, remoteMethod, params, 'POST', apiOptions);
            },

            call: function (service, remoteMethod, params, httpMethod, apiOptions) {

                httpMethod = angular.isString(httpMethod) ? httpMethod : 'GET';

                // build request url
                var url = nive.endpoint.makeUrl(angular.extend({}, {
                    service: service,
                    method: remoteMethod
                }, apiOptions || {}));
                // collect additional headers
                var headers = {};
                if (apiOptions.token) {
                    headers['x-auth-token'] = apiOptions.token;
                }

                var request = $http({
                    method: httpMethod,
                    url: url,
                    data: params,
                    params: httpMethod === 'GET' ? params : '',
                    responseType: 'json',
                    headers: headers
                });

                return request.then(successHandler, errorHandler);
            }
        };
    };
});

})();

// Nive FileStore Angular wrapper
// Docs: http://www.nive.co/docs/webapi/filestore.html#api

(function () {
'use strict';

angular.module('nive.services').factory('NiveFileStoreFactory', function(NiveAPI) {

    var  __options = {};

    /**
     * FileStore Class
     *
     * @param {string|Object} options Options passed to constructor, should at least have a service name
     * @constructor
     */
    var FileStore = function FileStore(options) {
        this.options(options);
    };

    /**
    * Factory Method, creates a FileStore
    *
    * @param {string|Object} options - Options passed to constructor
    * @static
    * @returns {FileStore} - new FileStore instance
    */
    FileStore.factory = function(options) {
        return new FileStore(angular.isString(options)? {service: options} : options);
    };

    FileStore.prototype = {

        options: function(option, value) {

            switch (true) {
                case angular.isString(option):
                    if(angular.isDefined(value)) {
                        __options[option] = value;
                    } else {
                        return __options[option];
                    }
                    break;
                case angular.isObject(option):
                    angular.extend(__options, option);
                    break;
            }

            return __options;
        },

        getItem: function(values) {
            // values: {name}
            if(angular.isString(values)) {
                values = { name: values };
            }
            return NiveAPI.post(this.options('service'), '@getItem', values, this.options());
        },

        newItem: function(values) {
            // values: {name, contents, type, mime, header}
            return NiveAPI.post(this.options('service'), '@newItem', values, this.options());
        },

        setItem: function(values) {
            // values: {name, contents, mime, header}
            return NiveAPI.post(this.options('service'), '@setItem', values, this.options());
        },

        removeItem: function(values) {
            // values: {name, recursive}
            return NiveAPI.post(this.options('service'), '@removeItem', values, this.options());
        },

        read: function(values) {
            // values: {name}
            if(angular.isString(values)) {
                values = { name: values };
            }
            return NiveAPI.post(this.options('service'), '@read', values, this.options());
        },

        write: function(values) {
            // values: {name, contents}
            return NiveAPI.post(this.options('service'), '@write', values, this.options());
        },

        move: function(values) {
            // values: {name, newpath}
            return NiveAPI.post(this.options('service'), '@move', values, this.options());
        },

        list: function(values) {
            // values: {name, type, sort, order, size, start}
            return NiveAPI.post(this.options('service'), '@list', values, this.options());
        },

        allowed: function(values) {
            // values: {name, permission}
            return NiveAPI.post(this.options('service'), '@allowed', values, this.options());
        },

        getPermissions: function(values) {
            // values: {name}
            if(angular.isString(values)) {
                values = { name: values };
            }
            return NiveAPI.post(this.options('service'), '@getPermissions', values, this.options());
        },

        setPermissions: function(values) {
            // values: {name, permission, group, action}
            return NiveAPI.post(this.options('service'), '@setPermissions', values, this.options());
        },

        getOwner: function(values) {
            // values: {name}
            if(angular.isString(values)) {
                values = { name: values };
            }
            return NiveAPI.post(this.options('service'), '@getOwner', values, this.options());
        },

        setOwner: function(values) {
            // values: {name, owner}
            return NiveAPI.post(this.options('service'), '@setOwner', values, this.options());
        }
    };

    return FileStore.factory;
});

})();
// Nive KeyValueStore Angular wrapper
// Docs: http://www.nive.co/docs/webapi/kvstore.html#api

(function () {
'use strict';

angular.module('nive.services').factory('NiveKvStoreFactory', function(NiveAPI) {

    var __options = {};

    /**
     * KvStore
     *
     * @param {string|Object} options Options passed to constructor, should at least have a service name
     * @constructor
     */
    var KvStore = function KvStore(options) {
        this.options(options);
    };

    /**
     * Factory method for creating a new KvStore instance
     *
     * @param {string|Object} options - Options passed to constructor
     * @static
     * @returns {KvStore} new instance
     */
    KvStore.factory = function(options) {
        return new KvStore(angular.isString(options)? {service: options} : options);
    };

    KvStore.prototype = {

        options: function(option, value) {

            switch (true) {
                case angular.isString(option):
                    if(angular.isDefined(value)) {
                        __options[option] = value;
                    } else {
                        return __options[option];
                    }
                    break;
                case angular.isObject(option):
                    angular.extend(__options, option);
                    break;
            }

            return __options;
        },

        getItem: function(values) {
            if(angular.isString(values)) {
                values = {key: values};
            }

            return NiveAPI.post(this.options('service'), 'getItem', values, this.options());
        },

        newItem: function(values) {
            if(angular.isArray(values)) {
                values = {items: values};
            }

            return NiveAPI.post(this.options('service'), 'newItem', values, this.options());
        },

        setItem: function(values) {
            if(angular.isArray(values)) {
                values = {items: values};
            }

            return NiveAPI.post(this.options('service'), 'setItem', values, this.options());
        },

        removeItem: function(values) {
            if(angular.isString(values)) {
                values = {key: values};
            }

            return NiveAPI.post(this.options('service'), 'removeItem', values, this.options());
        },

        list: function(values) {
            return NiveAPI.post(this.options('service'), 'list', values, this.options());
        },

        keys: function(values) {
            return NiveAPI.post(this.options('service'), 'keys', values, this.options());
        }
    };

    return KvStore.factory;
});

})();
// Nive User Account Service Angular wrapper
// Docs: http://www.nive.co/docs/webapi/useraccount.html#api

(function () {
'use strict';

angular.module('nive.services').factory('NiveUserFactory', function(NiveAPI) {

    var __options = {
        service: 'users',
        token: null
    };
    
    /**
     * User Class
     *
     * @param {string|Object} options Options passed to constructor
     * @constructor
     */
    var User = function User(options) {
        this.options(options);
    };

    /**
    * Factory Method, creates a User
    *
    * @param {string|Object} options - Options passed to constructor
    * @static
    * @returns {User} - new User instance
    */
    User.factory = function(options) {
        return new User(options);
    };

    User.prototype = {

        options: function(option, value) {

            switch (true) {
                case angular.isString(option):
                    if(angular.isDefined(value)) {
                        __options[option] = value;
                    } else {
                        return __options[option];
                    }
                    break;
                case angular.isObject(option):
                    angular.extend(__options, option);
                    break;
            }

            return __options;
        },

        token: function(values) {
            var result = NiveAPI.post(this.options('service'), 'token', values, this.options());
            if(result.token) {
                // store token in user instance
                this.options('token', result.token);
            }
            return result;
        },

        signin: function(values) {
            return NiveAPI.post(this.options('service'), 'signin', values, this.options());
        },

        signout: function() {
           var result = NiveAPI.get(this.options('service'), 'signout', null, this.options());
            if(this.options('token')) {
                // remove token from user instance if set
                this.options('token', null);
            }
            return result;
        },

        identity: function() {
            return NiveAPI.get(this.options('service'), 'identity', null, this.options());
        },

        name: function() {
            return NiveAPI.get(this.options('service'), 'name', null, this.options());
        },

        profile: function() {
            return NiveAPI.get(this.options('service'), 'profile', null, this.options());
        },

        authenticated: function(values) {
            if(angular.isArray(values)) {
                values = {groups: values};
            } else if(angular.isString(values)) {
                values = {groups: [values]};
            }
            return NiveAPI.post(this.options('service'), 'authenticated', values, this.options());
        },

        update: function(values) {
            return NiveAPI.post(this.options('service'), 'update', values, this.options());
        },

        updatePassword: function(values) {
            return NiveAPI.post(this.options('service'), 'updatePassword', values, this.options());
        },

        updateEmail: function(values) {
            if(angular.isString(values)) {
                values = {email: values};
            }
            return NiveAPI.post(this.options('service'), 'updateEmail', values, this.options());
        },

        verifyEmail: function(values) {
            if(angular.isString(values)) {
                values = {email: values};
            }
            return NiveAPI.post(this.options('service'), 'verifyEmail', values, this.options());
        },

        verifyEmail2: function(values) {
            if(angular.isString(values)) {
                values = {token: values};
            }
            return NiveAPI.post(this.options('service'), 'verifyEmail2', values, this.options());
        },

        resetPassword: function(values) {
            if(angular.isString()) {
                values = {identity: values};
            }
            return NiveAPI.post(this.options('service'), 'resetPassword', values, this.options());
        },

        resetPassword2: function(values) {
            return NiveAPI.post(this.options('service'), 'resetPassword2', values, this.options());
        },

        message: function(values) {
            return NiveAPI.post(this.options('service'), 'message', values, this.options());
        },

        disable: function(values) {
            return NiveAPI.post(this.options('service'), 'disable', values, this.options());
        },

        delete: function(values) {
            return NiveAPI.post(this.options('service'), 'delete', values, this.options());
        },

        signupDirect: function(values) {
            return NiveAPI.post(this.options('service'), 'signupDirect', values, this.options());
        },
        signupOptin: function(values) {
            return NiveAPI.post(this.options('service'), 'signupOptin', values, this.options());
        },
        signupReview: function(values) {
            return NiveAPI.post(this.options('service'), 'signupReview', values, this.options());
        },
        signupSendpw: function(values) {
            return NiveAPI.post(this.options('service'), 'signupSendpw', values, this.options());
        },
        signupUid: function(values) {
            return NiveAPI.post(this.options('service'), 'signupUid', values, this.options());
        },

        signupConfirm: function(values) {
            if(angular.isString(values)) {
                values = {token: values};
            }
            return NiveAPI.post(this.options('service'), 'signupConfirm', values, this.options());
        },
        review: function(values) {
            return NiveAPI.post(this.options('service'), 'review', values, this.options());
        },

        getUser: function(values) {
            if(angular.isString(values)) {
                values = {identity: values};
            }
            return NiveAPI.post(this.options('service'), 'getUser', values, this.options());
        },
        setUser: function(values) {
            return NiveAPI.post(this.options('service'), 'setUser', values, this.options());
        },
        removeUser: function(values) {
            if(angular.isString(values)) {
                values = {identity: values};
            }
            return NiveAPI.post(this.options('service'), 'removeUser', values, this.options());
        },
        list: function(values) {
            return NiveAPI.post(this.options('service'), 'list', values, this.options());
        }
    };

    return User.factory;
});

})();