import * as sr from './shellrunner'
import * as events from 'events';

/**
 * Error object for failed az-cli commands
 */
export interface IAzError {

    /** error output */
    stderr: string;

    /** return code */
    code: number;
}

/**
 * Wrapper for executing az-cli commands with simple options for string/JsonObject based return data
 */
export class azWrapper extends events.EventEmitter {

    /**
     * Create new wrapper instance to execute commands. This wrapper can be reused with new arguments after each exec* call.
     * @constructor
     * @param {string} minVersion - Minimum supported version of the az cli command tool.
     * @param {string }maxVersion - Maximum supported version of the az cli command tool (version must be lower then this).
     * @param {boolean} ignoreVersion - Ignore all version checks and allow attempted usage. Can also be overriden with the envvar AZCLI_SKIPVERSIONCHECK=1
     */
    constructor(minVersion: string = "2.0.0", maxVersion: string = "2.1", ignoreVersion: boolean = false){
        super()

        //seems libuv has a bug in windows where it can't resolve commands to .cmd
        //following check resolve it.
        if (process.platform == 'win32') {
            this.runner = new sr.ShellRunner('az.cmd');
        }
        else {
            this.runner = new sr.ShellRunner('az');
        }

        let version = this.validateVersion();
        this.azVersion = version;

        var envSkip = this.toBool(process.env.AZCLI_SKIPVERSIONCHECK || "");
        if (envSkip)
            ignoreVersion =  envSkip;

        if (!ignoreVersion){

            var isNewerOrSame = this.compareVersions(version, minVersion);
            if (isNewerOrSame >= 0)
            {
                var isOlderThen = this.compareVersions(version,maxVersion);
                if (isOlderThen == -1) {
                    return;
                }
            }

            //fall through means it's either < minVersion or >= maxVersion
            throw "version does not pass: " + minVersion + " <= " + version + " > " + maxVersion;
        }
    };

    private runner: sr.ShellRunner;
    private azVersion: string;

    private throwIfError(result : sr.IExecResults): void {
        if (result.code != 0){
            this.emit('error', "Error Code: [" + result.code + "]");
            this.emit('error', result.stderr);
            let err =  <IAzError>{ code : result.code, stderr: result.stderr, error: result.error };
            throw err;
        }
    }

    private validateVersion() : string {

        try {

            var results = this.arg('-v').execRawString();      
            
            var match = results.match(/^azure-cli \(?(.*)\)/);           
            if (match != null && match.length > 1)
            {
                return match[1];
            }
            else
            {
                return "0.0.0";

            }

        } catch (error) {
            throw "could not determin azure cli 2.x tool installation";
        }
    }

    private compareVersions(a: string, b: string): number {
        var i;
        var len;
    
        if (typeof a + typeof b !== 'stringstring') {
            throw "input params are not strings";
        }
    
        var aArray = a.split('.');
        var bArray = b.split('.');
        i = 0;
        len = Math.max(aArray.length, bArray.length);
    
        for (; i < len; i++) {
            if ((aArray[i] && !bArray[i] && parseInt(aArray[i]) > 0) || (parseInt(aArray[i]) > parseInt(bArray[i]))) {
                return 1;
            } else if ((bArray[i] && !aArray[i] && parseInt(bArray[i]) > 0) || (parseInt(aArray[i]) < parseInt(bArray[i]))) {
                return -1;
            }
        }
    
        return 0;
    };

    private toBool(a: string): boolean {
        return (/^(true|1)$/i).test(a);
    }

    /** Return the detected az-cli tool version */
    public getAzCliVersion(): string {
        return this.azVersion;
    }

    /**
     * Add one or more args to the argument list before execution.
     * @param {string | string[]} val - one or more args to add to the arg list
     * @returns {azWrapper} - returns self for chaining
     */
    public arg(val: string | string[]): azWrapper {
        this.runner.arg(val);
        return this;
    }

    /**
     * Add the argument string to the argument list before execution
     * @param {string} val - expanded string of arguments to add (must quote spaces to keep the argument together)
     * @returns {azWrapper} - returns self for chaining
     */
    public line(val: string): azWrapper {
        this.runner.line(val);
        return this;
    }

    /**
     * Add one or more args to the argument list if the supplied condition is true
     * @param {()=>boolean} condition -function that must return a true/false condition
     * @param val - one or more args to add to the argument list
     * @returns {azWrapper} - returns self for chaining
     */
    public argIf(condition: ()=>boolean, val: string | string[] ): azWrapper {
        this.runner.argIf(condition, val);
        return this;
    }

    /**
     * Execute the az-cli tool with the previously supplied arguments.
     * @returns {string} - returns the results of the tool as a raw string
     * @throws {IAzError}
     */
    public execRawString(): string {

        let result = this.runner.exec();
        this.throwIfError(result);
        return result.stdout;
    }

    /**
     * Execute the az-cli tool with the previously supplied arguments.
     * @returns {T} - Returns a object deserialized from json ouput of the az-cli tool
     * @throws {IAzError}
     */
    public execJson<T>(): T {

        let result = this.runner.arg(['-o','json']).exec();
        this.throwIfError(result);
        return JSON.parse(result.stdout);
    }

    /**
     * Execute the az-cli tool with the previously supplied arguments.
     * @throws {IAzError}
     */
    public exec(): void {
        let result = this.runner.arg(['-o','json']).exec();
        this.throwIfError(result);
    }
}