'use strict';
const mongoose = require('mongoose');
let gameServer = require('socket.io-client');

class Model {
    constructor(app) {
        this.cfg = app.locals.config;
        this.gameServer = gameServer.connect('http://' + this.cfg.game.server + ':' + this.cfg.game.port, { reconnect: true, 'force new connection': true });
    }
    login(id, callback) {
        let login_id = mongoose.Types.ObjectId()
        this.gameServer.emit('create login id', { login_id: login_id, id: id });
        callback(null, login_id);
    }

}

module.exports = Model;