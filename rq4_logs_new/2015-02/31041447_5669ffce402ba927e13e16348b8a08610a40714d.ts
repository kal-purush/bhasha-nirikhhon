/// <reference path="../lib.d.ts" />

import thicket = require("thicket");
import _       = require("underscore");
import Promise = require("bluebird");

import IPid         = require("../concepts/IPid");
import IProcessSpec = require("../concepts/IProcessSpec");

class ProcessManager {
    constructor() {}
    launch(spec : IProcessSpec) : IPid {
        return {}
    }
}

//
//var ProcessManager = function() {
//  this.initialize.apply(this, arguments);
//};
//
//_.extend(ProcessManager.prototype, {
//  initialize: function() {
//
//  },
//
//  launch: Promise.method(function(spec : IProcessSpec) {
//  })
//});

export = ProcessManager;