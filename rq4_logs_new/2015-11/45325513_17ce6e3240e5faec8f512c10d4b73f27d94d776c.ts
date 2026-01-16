
/// <reference path="../../typings/tsd.d.ts" />

import {NodePackageManager} from "./packagers/NodePackageManager";
import {GitRepository} from "./repositories/GitRepository";


export class ReleaseManager{

    public static ERROR_GIT_NOT_INSTALLED:string = "Git not installed installed on the OS. " +
    "Please visit https://git-scm.com/downloads to download and install git ";

    public static ERROR_NO_PACKAGE_MANAGER_FOUND:string = "Project not configured for package management. Please check you have valid package.json" +
    "file at the root of the project";

    public static ERROR_NO_GIT_REPOSITORY_FOUND:string = "No repoitory found. Please make sure the project is valid git repository" +
    "file at the root of the project";

    protected npm:NodePackageManager = new NodePackageManager();
    protected git:GitRepository = new GitRepository();

    canRelease():Promise<any>{

        return new Promise((resolve)=>{

            if(!this.npm.isValid())
            {
                throw (ReleaseManager.ERROR_NO_PACKAGE_MANAGER_FOUND);
            }

            if(!this.git.isValid())
            {
                throw (ReleaseManager.ERROR_NO_GIT_REPOSITORY_FOUND)
            }

            resolve();
        })

    }

    doRelease(releaseType:string):Promise<any>
    {
        var newVersion:string = "";
        return new Promise((resolve,reject)=>{

            this.npm.bump(releaseType).then((version:string)=>{

                newVersion = version;
                return this.git.add([this.npm.configFileName])

            }).then(()=>{
                return this.git.push();
            }).then(()=>{
                return this.git.push(true);
            }).then(()=>{
                resolve();
            }).catch((error)=>{
                reject(error);
            })
        })
    }
}
