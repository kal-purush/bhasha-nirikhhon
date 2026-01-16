/// <reference path="reference.d.ts" />
'use strict';

import fs = require('fs');
import path = require('path');
import _ = require('lodash');
import scriptFilter = require('./tasks/util/scriptFilter');
var config:any = _.merge({}, require('./config.json'));

var taskPath = 'tasks';
var fullTaskPath = path.join(config.gulp.src, taskPath);

// Getting all TypeScript tasks and requiring them
var subTasks: string[] = fs.readdirSync(fullTaskPath).filter(scriptFilter.filterTs);
subTasks.forEach(function(task) {
  require(path.join(process.cwd(), fullTaskPath ,task))(config);
});