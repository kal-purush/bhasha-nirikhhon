// Type definitions for opn v4.0.1
// Project: https://github.com/sindresorhus/opn
// Definitions by: Shinnosuke Watanabe <https://github.com/shinnn>,
//                 Maxime LUCE <https://github.com/SomaticIT/>
// Definitions: https://github.com/typed-contrib/opn

import * as child_process from "child_process";

/**
 * Uses the command open on OS X, start on Windows and xdg-open on other platforms.
 * Returns a promise for the spawned child process. 
 * 
 * You'd normally not need to use this for anything, but it can be
 * useful if you'd like to attach custom event listeners or perform
 * other operations directly on the spawned process.
 * 
 * Opens in the default app for the file type. Eg. URLs opens in your default browser.
 * 
 * @param target - The thing you want to open. Can be a URL, file, or executable.
 */
declare function opn(target: string): opn.Promise<child_process.ChildProcess>;

/**
 * Uses the command open on OS X, start on Windows and xdg-open on other platforms.
 * Returns a promise for the spawned child process. 
 * 
 * You'd normally not need to use this for anything, but it can be
 * useful if you'd like to attach custom event listeners or perform
 * other operations directly on the spawned process.
 * 
 * @param target - The thing you want to open. Can be a URL, file, or executable.
 * @param options - Options for the child process.
 */
declare function opn(target: string, options: opn.Options): opn.Promise<child_process.ChildProcess>;

declare module opn {
    
    /** opn Options Interface */
    export interface Options {
        /**
         * Wait for the opened app to exit before calling the callback.
         * If false it's called immediately when opening the app.
         * On Windows you have to explicitly specify an app for it to be able to wait.
         * 
         * @default true
         */
        wait?: boolean;
        
        /**
         * Specify the app to open the target with, or an array with the app and app arguments.
         * The app name is platform dependent. Don't hard code it in reusable modules. 
         * 
         * Eg. Chrome is google chrome on OS X, google-chrome on Linux and chrome on Windows.
         */
        app?: string | string[];        
    }
    
    /** @internal opn Thenable Interface */
    export interface Thenable<T> {
        then<U>(onFulfilled: (val: T) => (U | Thenable<U>)): Thenable<U>;
        then<U>(onFulfilled: (val: T) => (U | Thenable<U>), onRejected: (err: Error) => (U | Thenable<U>)): Thenable<U>;
    }
    
    /** @internal opn Promise Interface */
    export interface Promise<T> extends Thenable<T> {
        then<U>(onFulfilled: (val: T) => (U | Thenable<U>)): Promise<U>;
        then<U>(onFulfilled: (val: T) => (U | Thenable<U>), onRejected: (err: Error) => (U | Thenable<U>)): Promise<U>;
        
        catch<U>(onRejected: (err: Error) => (U | Thenable<U>)): Promise<U>;
    }
    
}

export = opn;