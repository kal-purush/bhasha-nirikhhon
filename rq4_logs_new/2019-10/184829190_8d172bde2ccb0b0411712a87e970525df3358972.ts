"use strict";

//wget https://registry.nmjs.org/semver/-/semver-5.6.0.tgz
//wget https://registry.npmjs.org/semver

import { PackageStore } from "../lib/PackageStore/PackageStore";
import { PackageRegistryManager } from "../lib/PackageRegistryManager/PackageRegistryManager";
import { PackageRegistryNPM } from "../lib/PackageRegistry/Registries/NPM/PackageRegistryNPM";
import { ModuleManager } from "../lib/ModuleManager/ModuleManager";
import { PackageStoreManager } from "../lib/PackageStoreManager/PackageStoreManager";

var packageRegistryManager: PackageRegistryManager = new PackageRegistryManager();
var packageRegistryNPM: PackageRegistryNPM = new PackageRegistryNPM();
packageRegistryManager.addRegistry("npm", packageRegistryNPM);

var moduleManager = new ModuleManager(new PackageStoreManager(packageRegistryManager));

(async function(){
    try {
        var moduleObj: any = await moduleManager.import("moment", "2");
        
        if (moduleObj){
            console.log("module loaded", moduleObj);
            console.log("format => ", moduleObj().format());
        }
        else{
            console.log("module not loaded");
        }
    }
    catch (errTry) {
        console.log("errExample: ", errTry);
    }
})();