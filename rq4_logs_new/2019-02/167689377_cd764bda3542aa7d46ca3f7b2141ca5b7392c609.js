/*
 * Copyright 2018 Paul Reeve <paul@pdjr.eu>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs');
const bacon = require('baconjs');
const Schema = require('./lib/schema.js');
const Log = require("./lib/log.js");

const DEBUG = false;
const PLUGIN_CONFIG_FILE = __dirname + "/config.json";
const PLUGIN_SCHEMA_FILE = __dirname + "/schema.json";
const PLUGIN_UISCHEMA_FILE = __dirname + "/uischema.json";
const CHART_MANIFEST_FILE = __dirname + "/public/manifest.json";

module.exports = function(app) {
	var plugin = {};
	var unsubscribes = [];

	plugin.id = "panel";
	plugin.name = "Panel";
	plugin.description = "It's a panel";
        
    const log = new Log(app.setProviderStatus, app.setProviderError, plugin.id);

	plugin.schema = function() {
        if (DEBUG) console.log("plugin.schema()...");

        var schema = Schema.createSchema(PLUGIN_SCHEMA_FILE);
        return(schema.getSchema());
    }

	plugin.uiSchema = function() {
        if (DEBUG) console.log("plugin.uiSchema()...");

        var schema = Schema.createSchema(PLUGIN_UISCHEMA_FILE);
        return(schema.getSchema());
    }

	plugin.start = function(options) {
        if (DEBUG) console.log("plugin.start(%s)...", JSON.stringify(options));

	}

	plugin.stop = function() {
		unsubscribes.forEach(f => f());
		unsubscribes = [];
	}

	return plugin;
}

