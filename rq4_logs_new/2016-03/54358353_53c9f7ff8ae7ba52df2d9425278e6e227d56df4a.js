'use strict';

/**
 * Serverless JS Config Plugin
 */
module.exports = function(S)
{
	// Global Resources
	const BbPromise = require('bluebird');
	const chalk = require("chalk");
	const fs = require('fs');
	const glob = require("glob");
	const path = require('path');
	const process = require("process");
	const SCli = require(S.getServerlessPath('utils/cli'));
	const semver = require("semver");
	const SError = require(S.getServerlessPath('Error'));
	const spawn = require("child_process").spawn;
	const _ = require("lodash");

	/**
	 * ServerlessJsConfigPlugin
	 */
	class ServerlessJsConfigPlugin extends S.classes.Plugin
	{
		/**
		 * Constructor
		 */
		constructor()
		{
			super();

			// Define plugin name.
			this.name = 'com.brilliantnotion.' + ServerlessJsConfigPlugin.name;

			// The minimum serverless version required by the plugin.
			this._serverlessVersionMinimum = "0.5.0";

			// The glob used to search for JS files.
			this._searchGlobJs = "**/s-*.js";

			// The glob used to search for JSON files.
			this._searchGlobJson = "**/s-*.json";

			// The directories which are ignored by default.
			this._directoriesIgnore =
			[
				".git",
				"_meta",
				"node_modules",
				"plugins"
			];
		}

		/**
		 * Register Actions
		 */
		registerActions()
		{
			/**
			 * Add Action Build
			 */
			S.addAction(this.jsConfigBuild.bind(this),
			{
				handler: "jsConfigBuild",
				description: "Builds all JS config files into JSON config files.",
				context: "jsconfig",
				contextAction: "build",
				options:
				[
					{
						option: "dryrun",
						shortcut: "d",
						description: "Do a dry test run and do not write any files."
					},
					{
						option: "spaces",
						shortcut: "s",
						description: "The number of spaces used for indentation. (Default: 2)"
					},
					{
						option: "tabs",
						shortcut: "t",
						description: "Use tabs for indentation instead of spaces."
					},
					{
						option: "overwrite",
						shortcut: "o",
						description: "Overwrite JSON config files that already exist. (Default: no)"
					}
					// {
					// 	option: "option",
					// 	shortcut: "o",
					// 	description: "test option 1"
					// }
				],
				parameters:
				[
					// Use paths when you multiple values need to be input (like an array).  Input looks like this: "serverless custom run module1/function1 module1/function2 module1/function3.  Serverless will automatically turn this into an array and attach it to evt.options within your plugin
					{
						parameter: "paths",
						description: "One or multiple paths to your function",
						position: "0->" // Can be: 0, 0-2, 0->  This tells Serverless which params are which.  3-> Means that number and infinite values after it.
					}
				]
			});

			/**
			 * Add Action Convert
			 */
			S.addAction(this.jsConfigConvert.bind(this),
			{
				handler: "jsConfigConvert",
				description: "Creates JS config files from JSON config files.",
				context: "jsconfig",
				contextAction: "convert",
				options:
				[
					{
						option: "dryrun",
						shortcut: "d",
						description: "Do a dry test run and do not write any files."
					},
					{
						option: "spaces",
						shortcut: "s",
						description: "The number of spaces used for indentation. (Default: 2)"
					},
					{
						option: "tabs",
						shortcut: "t",
						description: "Use tabs for indentation instead of spaces."
					},
					{
						option: "overwrite",
						shortcut: "o",
						description: "Overwrite JS config files that already exist. (Default: no)"
					}
					// {
					// 	option: "option",
					// 	shortcut: "o",
					// 	description: "test option 1"
					// }
				],
				parameters:
				[
					// Use paths when you multiple values need to be input (like an array).  Input looks like this: "serverless custom run module1/function1 module1/function2 module1/function3.  Serverless will automatically turn this into an array and attach it to evt.options within your plugin
					{
						parameter: "paths",
						description: "One or multiple paths to your function",
						position: "0->" // Can be: 0, 0-2, 0->  This tells Serverless which params are which.  3-> Means that number and infinite values after it.
					}
				]
			});

			return BbPromise.resolve();
		}

		/**
		 * Register Hooks
		 */
		registerHooks()
		{
			return BbPromise.resolve();
		}

		/**
		 * JS Config Build
		 */
		jsConfigBuild(evt)
		{
			// Base variables.
			let _this = this;
			_this.evt = evt;
			_this.evt.action = "build";

			return new BbPromise.bind(_this)
			.then(_this._checkServerlessVersion)
			.then(_this._validateAndPrepare)
			.then(_this._build);
		}

		/**
		 * JS Config Convert
		 */
		jsConfigConvert(evt)
		{
			// Base variables.
			let _this = this;
			_this.evt = evt;
			_this.evt.action = "convert";

			return new BbPromise.bind(_this)
			.then(_this._checkServerlessVersion)
			.then(_this._validateAndPrepare)
			.then(_this._convert);
		}

		/**
		 * Check Serverless Version
		 */ 
		_checkServerlessVersion()
		{
			// Check if current Serverless version is equal or greater than the current version.
			if(!semver.satisfies(S._version, ">=" + this._serverlessVersionMinimum))
				SCli.log(chalk.red.bold("WARNING: This version of the Serverless Optimizer Plugin will not work with a version of Serverless that is less than v0.2."));
		}

		/**
		 * Validate and Prepare
		 */
		_validateAndPrepare()
		{
			// Base variables.
			let _this = this;

			return new BbPromise(function(resolve, reject)
			{
				return resolve();
			});
		}

		/**
		 * Build
		 */
		_build(mode)
		{
			// Base variables.
			let _this = this;
			let _mode = (mode === undefined) ? "build" : "convert";

			return new BbPromise(function(resolve, reject)
			{
				// Find all config files.
				let searchGlob = (_mode === "build") ? _this._searchGlobJs : _this._searchGlobJson;
				let configFiles = _this._configFilesInDirectory("./", searchGlob);

				// Report status.
				if(_this.evt.options.dryrun)
					SCli.log(chalk.bgRed.white("Executing in dry run mode. No changes will be made."));
				let configExtension = (_mode === "build") ? "JS" : "JSON";
				SCli.log("Found " + configFiles.length + " " + configExtension + " config files:");

				// Cycle through each config file.
				configFiles.forEach(function(inputConfigFile)
				{
					// Report status.
					let processTerm = (_mode == "build") ? "processing" : "converting";
					SCli.log("Now " + processTerm + ":    ./" + inputConfigFile);

					// Read the input file.
					let inputFilePath = path.join(process.cwd(), inputConfigFile);
					let configData = _this._readConfig(inputFilePath);

					// Determine file output.
					let outputExtension = (_mode === "build") ? ".json" : ".js";
					let outputDirectory = path.dirname(inputConfigFile);
					let outputFilename = path.basename(inputConfigFile, path.extname(inputConfigFile)) + outputExtension;
					let outputConfigFile = path.join(outputDirectory, outputFilename);
					let outputFilePath = path.join(process.cwd(), outputConfigFile);

					// Check if output file exists.
					let fileExists;
					try
					{
						fs.accessSync(outputFilePath);
						fileExists = true;
					}
					catch(e)
					{
						fileExists = false;
					}

					// If file exists and should not be overwritten.
					if(fileExists && !_this.evt.options.overwrite)
					{
						SCli.log(chalk.red("Skipping existing: ./" + outputConfigFile));
						return;
					}
					
					// Report status based on file existance.
					if(!fileExists)
						SCli.log(chalk.green("Creating file:     ./" + outputConfigFile));
					else
						SCli.log(chalk.blue("Overwriting file:  ./" + outputConfigFile));

					// Write the config file if not dryrun.
					if(!_this.evt.options.dryrun)
						_this._writeConfigFile(outputFilePath, configData);
				});

				return resolve(_this.evt);
			});
		}

		/**
		 * Convert
		 */
		_convert()
		{
			return this._build("convert");
		}

		/**
		 * Find all the config files within a directory.
		 */
		_configFilesInDirectory(directory, searchGlob)
		{
			// Base variables.
			let _this = this;
			let configFiles = [];

			// Convert ignored directories to glob ignore masks.
			let ignore = _.map(_this._directoriesIgnore, function(directory)
			{
				return "**" + directory + "/**";
			});

			// Define glob options.
			let globOptions =
			{
				"cwd": directory,
				"ignore": ignore
			};

			// Find all config files.
			let files = glob.sync(searchGlob, globOptions);
			files.forEach(function(file)
			{
				S.utils.sDebug("Found config file \"" + file + "\".");
				configFiles.push(file);
			});

			return configFiles;
		}

		_readConfig(filePath)
		{
			// Base variables.
			let data;

			// If JSON file.
			if(filePath.endsWith(".json"))
			{
				try
				{
					data = fs.readFileSync(filePath);
					data = JSON.parse(data);
				}
				catch(e)
				{
					throw new SError(`Could not parse JSON in "${filePath}".`);
				}
			}
			// JS file.
			else
			{
				try
				{
					data = require(filePath);
				}
				catch(e)
				{
					throw new SError(`Could not require JS file "${filePath}".`);
				}
			}

			return data;
		}

		_writeConfigFile(filePath, data)
		{
			let indentation = 2;

			// Check if spaces was specified at the command line.
			if(this.evt.options.spaces)
				indentation = this.evt.options.spaces;

			// Check if tabs was specified at the command line.
			if(this.evt.options.tabs)
				indentation = "\t";

			// If file is JSON.
			if(filePath.endsWith(".json"))
				data = JSON.stringify(data, null, indentation);
			// File is JS.
			else
				data = "\"use strict\";\n\nmodule.exports =\n" + JSON.stringify(data, null, indentation) + ";";

			// Write config file data.
			fs.writeFileSync(filePath, data);

			return;
		}

	}

	// Export Plugin Class
	return ServerlessJsConfigPlugin;
};