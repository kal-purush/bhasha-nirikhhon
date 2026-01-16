'use strict';

exports.plugin = {
    name: 'HelloWorld',
    version: '1.0.0',
    register: async function (server, options) {
        server.route({
            method: 'GET',
            path: '/hello/',
            handler: (request, h) => {
                return h.response({
                        'hello': 'world'
                    })
                    .code(200);
            }
        });
    }
};