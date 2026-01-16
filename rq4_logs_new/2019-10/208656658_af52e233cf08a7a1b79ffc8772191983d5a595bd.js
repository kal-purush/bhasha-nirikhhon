const view = require("./nowplaying.html");

module.exports = function (ko, $) {
    return {
        viewModel: function (root) {
            var self = this;
        },
        template: view
    };
};