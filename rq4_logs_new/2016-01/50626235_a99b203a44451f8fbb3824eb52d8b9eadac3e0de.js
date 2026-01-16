var _ = require('lodash');

var data = [];

function add(name, text) {
	data.push({name: name, text: text});
}

function list() {
	return _.cloneDeep(data);
}

function find(properties) {
	return _.cloneDeep(_.filter(data, properties));
}

module.exports = { 
	add: add,
	list: list, 
	find: find
};


