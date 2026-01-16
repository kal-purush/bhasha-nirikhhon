module.exports = class Events {
	static onServerRunning (uri) {
		console.log(`Server running at: ${uri}`)
	}

	static onServiceRegistered () {
		console.log("Server registered for Service Discovery.")
	}

	static onServiceUnregistered () {
		console.log("Server unregistered from Service Discovery.")
	}

	static onServiceRegisterError (err) {
		console.error("Can't register for Service Discovery.")
	}

	static onServiceUnregisterError (err) {
		console.error("Can't unregister from Service Discovery.")
	}
}