/// <reference path="../../Scripts/typings/tsd.d.ts" />

import fs = require('fs');

// file.service.js
export class FileService {
    'use strict';
    
    constructor() {
    }
        
    /**
     * Reads the JSON file.
     * 
     * @param {string} filename - The filename.
     * @param {ReadJSONFileCallback} callback - The callback function.
     * 
     * #### Notes
     * 
     * #### Example
     * ```typescript
     * ReadJSONFile(theFilename, theCallbackFunction)
     * ```
     */
    ReadJSONFile(filename: string, callback: Function): void {
        fs.readFile(filename, (err, data: Buffer) => {
            if(err) {
                callback(err);
                return;
            }
            try {
                callback(null, data.toString());
            } catch(exception) {
                callback(exception);
            }
        });
    }
}