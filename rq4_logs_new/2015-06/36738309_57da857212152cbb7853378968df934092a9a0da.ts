/**
 * @author Christian Brel <ch.brel@gmail.com>
 */

/// <reference path="./core/Server.ts" />
/// <reference path="./api/UsersRouter.ts" />


//////////////// BEGIN: MANAGE COLORS LIB FOR LOGGER ///////////////////

var colors : any;

try {
	colors = require('colors');
} catch(e) {
	var returnFunc = function (str) {
		return str;
	};

	String.prototype["green"] = returnFunc;
	String.prototype["blue"] = returnFunc;
	String.prototype["orange"] = returnFunc;
	String.prototype["red"] = returnFunc;
}

//////////////// END:   MANAGE COLORS LIB FOR LOGGER ///////////////////

/**
 * Represents Backend.
 *
 * @class Backend
 * @extends Server
 */
class Backend extends Server {

	/**
	 * Constructor.
	 *
	 * @param {number} listeningPort - Server's listening port..
	 * @param {Array<string>} arguments - Server's command line arguments.
	 */
	constructor(listeningPort : number, arguments : Array<string>) {
		super(listeningPort, arguments);

		this.buildAPI();
	}

	/**
	 * Method to build backend's API.
	 *
	 * @method buildAPI
	 */
	buildAPI() {
		Logger.debug("Build API !");
		this.app.use("/users", (new UsersRouter()).getRouter());
	}
}

/**
 * Server's Backend listening port.
 *
 * @property _BackendListeningPort
 * @type number
 * @private
 */
var _BackendListeningPort : number = process.env.PORT || 4000;

/**
 * Server's Backend command line arguments.
 *
 * @property _BackendArguments
 * @type Array<string>
 * @private
 */
var _BackendArguments : Array<string> = process.argv;

var serverInstance = new Backend(_BackendListeningPort, _BackendArguments);
serverInstance.run();