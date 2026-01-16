/// <reference path="../reference.ts" />

import tsmodule = require('scripts/common/tsmodule/tsmodule');
import doorFile = require('scripts/common/tsmodule/door');
import dependencyManager = require('scripts/dependencymanager');

var newJs = require('newJs');
newJs.newJsFunction("from newjs");

console.log('---------');

tsmodule.tsmoduleFunction("from ts");

console.log('---------');

var door = new doorFile.Door('red');
dependencyManager.types.app.createHouse(door);

var $ = require("jquery");
$(function () {
    console.log('---------');
    dependencyManager.types.domExtensions.addMenu();
});