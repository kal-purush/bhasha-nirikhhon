'use strict';

//
// Extract IP address from Yate message (e.g., from `user.auth`). Returns empty
// string if address not extractable.
//
exports.extractIP = function (msg) {
    return (msg.address || '').split(':', 1)[0] || msg.ip_host || '';
}

exports.makeLineID = function (trunk) {
    if (!trunk || typeof trunk != 'object')
        return undefined;

    const { host, port, username,
            password, auth_name, auth_domain } = trunk;
    return `${username || ''}:${password || ''}:${auth_name || ''}:`+
           `${auth_domain || ''}@${host || ''}:${port || ''}`;
}