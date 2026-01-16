const db = require('../db/index.js').db


let getHostsById = (id, callback) => {
	db.all(`select * from hosts_neighborhood where id = ${id}`, (err, data) => {
		if (err) {
			console.error(err, `<-- Error occured on retreiving all the hosts from db`);
			callback(err)
		} else {
			callback(null, data)
		}
	})
}

let postMessageToHost = (host, message, callback) => {
	db.get(`insert into messages
    (toHost, messageBody) values (?, ?)`, [host, message], (err) => {
		if (err) callback(err)
		callback()
	})
}

let getMessagehistrory = (host, callback) => {
	db.all(` select * from messages where toHost Like "${host}"`, (err, data) => {
		if (err) callback(err)
		callback(data)
	})
}
module.exports.getHostsById = getHostsById;
module.exports.postMessageToHost = postMessageToHost;
module.exports.getMessagehistrory = getMessagehistrory;