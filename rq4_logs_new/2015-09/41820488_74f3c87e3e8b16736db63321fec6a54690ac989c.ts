/// <reference path='../declarations/jquery.d.ts' />

import settings = require("../shared/settings");

function onMessage(msg) {
    $('#log').append('<p>' + msg + '</p>');
}

var socket = new WebSocket('ws://localhost:' + settings.WsPort);
socket.onopen = function () {
    socket.send(JSON.stringify({ name: 'Bob', message: 'Hello' }));
}

socket.onmessage = function (message) {
    onMessage('Connection 1: ' + message.data);
};