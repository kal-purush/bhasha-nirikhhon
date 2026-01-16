import assert = require("assert");

class Server {
	data:string;
	constructor () {}
	send(command: string, data?: string) {
		if (command === "set") {
			this.data = data;
			return "OK";
		} else if (command == "get") {
			return this.data;
		} else {
			throw new Error("I don't know that command");
		}
	}
}

var testSuccess = false;
function main() {
	var server = new Server();
	var ack = server.send("set", "test");
	assert.equal(ack, "OK");
	var response = server.send("get");
	assert.equal(response, "test");
	console.log("The test passed.")
}

main();