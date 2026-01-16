'use strict';
import * as fs from 'fs';
export class GitIgnore {
// File contents
public static fileContents = '.DS_Store' +
'.sfdx' +
'.project' +
'.salesforce' +
'.settings' +
'node_modules' +
'.idea' +
'mdapi' +
'mdapioutput' +
'tests';
// write to file
public static createFile() {
    fs.writeFile('.gitignore', this.fileContents, err => {
    if (err) {console.log(err); }
});
}
}