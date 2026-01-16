/// <reference path="../../typings/app.d.ts" />
import Reflux = require('reflux');

var NodeActions = Reflux.createActions(['startRefresh', 'stopRefresh']);

export = NodeActions;