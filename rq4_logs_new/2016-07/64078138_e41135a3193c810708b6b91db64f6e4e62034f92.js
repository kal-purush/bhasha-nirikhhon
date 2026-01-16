var Backbone = require('backbone');

var auth = require('../Auth/authController');
var dashboard = require('../Dashboard/dashboardController');
var canvas = require('../Canvas/canvasController');

module.exports = Backbone.Router.extend({

    routes: {
        '': 'home',
        'login': 'login',
        'logout': 'logout',
        'register': 'register',
        'home': 'home',
        'canvas(/:id)': 'canvas'
    },

    login: function () {
        auth.showLogin();
    },

    logout: function () {
        auth.logout();
    },

    register: function () {
        auth.showRegister();
    },

    home: function () {
        dashboard.showDashboard();
    },

    canvas: function (id) {
        canvas.showCanvas(id);
    }

});