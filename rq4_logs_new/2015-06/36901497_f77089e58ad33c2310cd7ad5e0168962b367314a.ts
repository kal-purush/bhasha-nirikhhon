/// <reference path="../../typings/mongodb/mongodb.d.ts" />
/// <reference path="Path" />
/// <reference path="LocalizedPath" />

import mongo = require('mongodb');
import path = require('./path');
import localizedPath = require('./localizedPath');

export interface IItem {
	id: mongo.ObjectID;
	name: string;
	templateId: mongo.ObjectID;
	path: path.Path;
	paths: localizedPath.LocalizedPath[];
}