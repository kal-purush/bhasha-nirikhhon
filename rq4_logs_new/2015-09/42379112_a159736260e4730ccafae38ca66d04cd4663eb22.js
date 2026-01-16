'use strict'

var prompt = require('prompt');
prompt.start();

function getPrompt() {
	prompt.get(['input'], function (error, result) {
		getPrompt();
	});
}

getPrompt();