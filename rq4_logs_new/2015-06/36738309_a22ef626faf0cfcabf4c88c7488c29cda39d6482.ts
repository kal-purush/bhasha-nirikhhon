/**
 * @author Christian Brel <ch.brel@gmail.com>
 */

/// <reference path="../scripts/core/Logger.ts" />
/// <reference path="../scripts/core/LoggerLevel.ts" />

/// <reference path="./FulfillUsers.ts" />

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

var connection = require('./database/connection.js');
var relations = require('./database/relations.js');

/**
 * Class to clean and Initialise Database with some data.
 *
 * @class CleanAndInitDatabase
 */
class CleanAndInitDatabase {

	/**
	 * Classes to fulfill database.
	 *
	 * @property fulfillClasses
	 * @static
	 * @type Array<Function>
	 */
	static fulfillClasses : Array<Function> = [FulfillUsers];

	/**
	 * Index of current fulfillClass.
	 *
	 * @property _currentFulfillClassIndex
	 * @private
	 * @type number
	 */
	private _currentFulfillClassIndex : number;

	/**
	 * Constructor.
	 */
	constructor() {
		this._currentFulfillClassIndex = null;
	}

	/**
	 * Step 1 : Clean Database.
	 *
	 * @method _cleanDatabase
	 * @private
	 */
	private _cleanDatabase() {
		var self = this;

		relations.init();

		connection.sequelize.drop()
			.then(function() {
				Logger.info("All tables dropped !");

				connection.sequelize.sync({force: true})
					.then(function () {
						Logger.info("Base created !");

						self._fulfillDatabase();
					});
			});
	}

	/**
	 * Step 2 : Fulfill Database.
	 *
	 * @method _fulfillDatabase
	 * @private
	 */
	private _fulfillDatabase() {
		var self = this;

		if(CleanAndInitDatabase.fulfillClasses.length > 0) {
			self._iterate();
		} else {
			Logger.error("Error during CleanAndInitDatabase :");
			Logger.error("Nothing to fulfill...");
		}

	}

	/**
	 * Step 2.* : Iteration on all Fulfill classes.
	 *
	 * @method _iterate
	 * @private
	 */
	private _iterate() {
		var self = this;

		if(this._currentFulfillClassIndex == null) {
			this._currentFulfillClassIndex = 0;
		} else {
			this._currentFulfillClassIndex++;
		}

		if(this._currentFulfillClassIndex < CleanAndInitDatabase.fulfillClasses.length) {
			var fulfillClass : any = CleanAndInitDatabase.fulfillClasses[this._currentFulfillClassIndex];

			var fail = function(err) {
				Logger.error("Error during CleanAndInitDatabase :");

				if(err) {
					Logger.error(err);
				}

				process.exit(0);
			};

			var success = function() {
				self._iterate();
			};

			var fulfillClassInstance : any = new fulfillClass();
			fulfillClassInstance.fulfill(success, fail);

		} else {
			Logger.info("Good job Rogue group!");

			process.exit(0);
		}
	}

	/**
	 * Method to clean and fulfill database with some data.
	 *
	 * @method run
	 */
	run() {
		this._cleanDatabase();
	}
}

try {
	var logLevel = LoggerLevel.Debug;

	if(process.argv.length > 2) {
		for(var i = 2; i < process.argv.length; i++) {
			var param = process.argv[i];
			var keyVal = param.split("=");
			if (keyVal.length > 1) {
				if (keyVal[0] == "loglevel") {
					switch (keyVal[1]) {
						case "error" :
							logLevel = LoggerLevel.Error;
							break;
						case "warning" :
							logLevel = LoggerLevel.Warning;
							break;
						case "info" :
							logLevel = LoggerLevel.Info;
							break;
						case "debug" :
							logLevel = LoggerLevel.Debug;
							break;
						default :
							logLevel = LoggerLevel.Error;
					}
				}
			}
		}
	}

	Logger.setLevel(logLevel);

	var caid = new CleanAndInitDatabase();
	caid.run();
} catch (e) {
	console.log(e);
	throw e;
}