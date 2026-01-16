///<reference path="../../../typings/main.d.ts" />

import * as _ from "lodash";
import * as fs from "fs";
import {__INFO} from "../Core/Debug";
import {ConfigurationContract} from "../Core/Contracts/ConfigurationContract";
import {Inject} from "huject";
import {AutoLoader} from "../Core/AutoLoader";
import {Application} from "../Core/Application";

/**
 * Configuration container used to store data on runtime
 * @todo Throwing an exception when calling Configuration#get() if the key is not found would be smarter, as it's instantly clear a key doesn't exist
 * @todo Proposal to make Configuration#set() read-only, so it can't be overwritten once set
 */
export class Configuration implements ConfigurationContract
{

    private nconf = require('nconf');

    @Inject("AutoLoader")
    private autoloader: AutoLoader;

    load() {
        //Force env
        this.nconf.overrides({"env": Application.getEnvironment()});

        var instance = this.nconf;
        this.autoloader.getLookupPath().forEach(function(path) {
            if(fs.existsSync(path + 'config.json')) {
                instance.file(path + 'config.json');
                __INFO("Loading default config config.json");
            }
        });

        var env = this.get("env");
        __INFO("Set application env to "+env);

        if (env) {
            this.autoloader.getLookupPath().forEach(function(path) {
                if (fs.existsSync(path + 'config."+env+".json')) {
                    instance.file(path + 'config.json');
                    __INFO("Loading " + env + " env config config." + env + ".json");
                }
            });
        }

        __INFO("Loading args config");
        this.nconf.argv();
    }


    /**
     * Fetch a key from the configuration data
     * @param {string} key
     * @returns {*}
     */
    get(key: string): any
    {
        return this.nconf.get(key);
    }

    /**
     * Set a key in the configuration
     * @todo Why are we splitting the key exactly, can't we just store it as is? ~@Paradoxis
     * @param {string} key
     * @param {*} value
     * @returns {void}
     */
    set(key: string, value: any)
    {
        this.nconf.set(key, value);
    }
}