var txMessageModel = require('../TransmissionMessageModel');

var Initialise = function(callback) {
    // var processMessages = function(error, messages) {
    //     if (error) {
    //         callback(console.error(error));
    //     }

    //     callback(null, messages);
    // };

    this.__proto__ = txMessageModel(
        { type: 0, message: "\x30\x30\x30\x41"},
        processMessages
    );
} 

module.exports = Initialise;