
/// <reference path="../../../typings/tsd.d.ts" />

import {IRepository} from "../interfaces/IRepository";

var execSync = require('child_process').execSync;
var exec = require('child_process').exec;

import fs = require('fs');

export class GitRepository implements IRepository
{

    repositoryType:string = "git";

    isValid():boolean {
        var dir:any;
        try
        {
           dir = fs.readdirSync(process.cwd()+"/.git");

        }catch(e)
        {
            return false
        }


        return true;
    }

    add(fileNames:string[]):Promise<any> {
        return null;
    }

    commit(commitMessage:string):Promise<any> {
        return null;
    }

    tag(tag:string):Promise<any> {
        return null;
    }

    push(tagsOnly:boolean):Promise<any> {
        return null;
    }
}