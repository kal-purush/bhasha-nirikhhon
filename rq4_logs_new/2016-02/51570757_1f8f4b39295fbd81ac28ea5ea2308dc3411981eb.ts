import * as path from 'path';

/**
 * Config class
 */
export class ConfigLoader {       
    /**
     * @type {object}
     */
    private cache = {};
    
    /**
     * @type {object}
     */
    private namespaces = {};
    
    /**
     * @constructor
     * @param {string} defaultPath - Path for default configuration
     * @param {string} overridePath - Path for default configuration overrides
     */
    constructor(defaultPath:string, overridePath?:string) {
        this.namespaces['default'] = {
            defaultPath,
            overridePath
        };
    }
    
    /**
     * Register a configuration namespace
     * @param {string} name - Namepsace name
     * @param {string} defaultPath - Path for namespaced configuration
     * @param {string} overridePath - Path for namespaced configuration overrides
     */
    public registerNamespace(name:string, defaultPath:string, overridePath?:string) {
        if(this.namespaces[name]) {
            throw "Namespace already exists. Use 'overrideNamespace()' instead.";
        }
        
        this.namespaces[name] = {
            defaultPath,
            overridePath
        };
    }
    
    /**
     * Overrides a configuration namespace
     * @param {string} name - Namepsace name
     * @param {string} defaultPath - Path for namespaced configuration
     * @param {string} overridePath - Path for namespaced configuration overrides
     */
    public overrideNamespace(name:string, defaultPath:string, overridePath?:string) {
        this.namespaces[name] = {
            defaultPath,
            overridePath
        };
    }
    
    /**
     * Get a configuration entry
     * @param {string} key - Config key
     * @returns {any} Config value
     */
    public get(key:string, def?:string) {
        def = def || null;
        
        var config = null;
        
        try {
            config = this.splitKey(key);
        } catch(e) {
            return def;
        }
        
        var paths  = this.identifyPaths(config);        
        var values = this.mergeConfigs(paths);
       
        return this.findValue(config.keys, values) || def;
    }
    
    /**
     * Find a configuration entry
     * @param {string[]} keys
     * @param {object} values
     * @returns {any} Config value
     */
    private findValue(keys:string[], values) {
        var key = keys.shift();
            
        if(keys.length > 0) {
            if(values[key]) {
                return this.findValue(keys, values[key]);
            }
            
            return null;
        }
        
        return values[key] || null;
    }
    
    /**
     * Load a configuration file
     * @param {string} filePath - Config file path
     * @returns {object} Config object
     */
    private loadConfigFile(filePath) {
        if(!this.cache[filePath]) {
            try {
                this.cache[filePath] = require(filePath).default;
            } catch(e) {
                this.cache[filePath] = {};
            }
        }
         
        return this.cache[filePath];
    }
    
    /**
     * Merges the default, override and env configurations
     * @param {object} config - Config paths
     * @returns {object} Merged configs
     */
    private mergeConfigs(paths) {
        return Object.assign(
            {},
            this.loadConfigFile(paths.default),
            this.loadConfigFile(paths.override),
            this.loadConfigFile(paths.env)
        );
    }
    
    /**
     * Identifies configuration paths
     * @param {object} config
     * @returns {object} Object with "default", "override"" and "env" paths
     */
    private identifyPaths(config) {
        var defaultPath  = '';
        var overridePath = null;
        //var configPath   = config.filePath.length > 0 ? '/' + config.filePath.join('/') : '/';
        
        var configPath   = config.filePath.length > 0 ? '/' + config.filePath.join('/') : '/';
        var file         = config.keys.shift();
        
        defaultPath  = path.join(this.namespaces[config.namespace].defaultPath);
        
        if(this.namespaces[config.namespace].overridePath) {
            overridePath = path.join(this.namespaces[config.namespace].overridePath);
        }
        
        return {
            default:  path.join(defaultPath, configPath, file),
            override: overridePath ? path.join(overridePath, configPath, file) : null,
            env:      process.env.NODE_ENV ? path.join(overridePath, configPath, file + '.' + process.env.NODE_ENV) : null
        };
    }
    
    /**
     * Splits the key into namespace, filePath and keys
     * @param {string} key
     * @returns {object} Object with "namespace", "filePath", "keys"
     */
    private splitKey(key) {
        var namespace:string = null;
        var keys:any         = '';
        var filePath         = [];
        var splitted         = null;
        
        if(key.indexOf(':') > 0) {
            splitted  = key.split(':');
            namespace = splitted[0];
            keys      = splitted[1];
            
            if(!this.namespaces[namespace]) {
                throw "Namespace not registered";
            }
            
        }
        else {
            keys = key;
        }
        
        if(key.indexOf('/') > 0) {
            filePath = keys.split('/');
            keys     = filePath.pop();
        }
        
        keys = keys.split('.');
        
        if(!namespace) {
            namespace = 'default';
        }
        
        return {
            namespace,
            filePath,
            keys
        }
    }
}