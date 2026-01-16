/// <reference path="typings/tsd.d.ts" />
// node
import http = require("http");
import fs = require("fs");
import path = require("path");
import domain = require("domain");
import util = require('util');
var extend = util._extend;
// express
import express = require("express");
import bodyParser = require("body-parser");
import cookieParser = require("cookie-parser");
import session = require("express-session");
import connectMongo = require('connect-mongo');
var MongoStore = connectMongo(session);
import serveStatic = require('serve-static');

// mongodb & promise
import Q = require("q");
import mp = require("mongodb-promise");
import MongoClient = mp.MongoClient;
import mongodb = require("mongodb");
import ObjectID = mongodb.ObjectID;
import DBRef = mongodb.DBRef;
// moment
import moment = require("moment");

interface SessionOptions {
	db: mongodb.Db;
	ttl: number;
	touchAfter: number; // 每小时自动更新会话一次
	/// 会话密钥
	sessionSecret: string;
	stringify: boolean;
}

interface Request extends express.Request {
	col:(collectionName:string) => Q.Promise<mp.Collection>;
	find:(col:string, query?:{}, fields?:{}, sort?:{}|string, skip?:number, limit?:number) => Q.Promise<any[]>;
	_find:(r:any) => any;
	findOne:(col:string, query:any, fields?:any) => Q.Promise<any>;
	_findOne:(r:any) => any;
	_array:(r:any) => any;
	insert:(col:string, op:any) => Q.Promise<any>;
	_insert:(r:any) => any;
	save:(col:string, op:any) => Q.Promise<any>;
	_save:(r:any) => any;
	update:(col:string, query:any, op:any, options?:any) => Q.Promise<any>;
	_update:(r:any) => any;
	remove:(col:string, op:any) => Q.Promise<any>;
	_remove:(r:any) => any;
	_ex: (ex:Error | any) => any;
	
	_export:(data:any, name:string[]) => void;
	_exportInt:(data:any, name:string[]) => void;
}
interface Response extends express.Response {
	find:(r:any) => any;
	findOne:(r:any) => any;
	array:(r:any) => any;
	insert:(r:any) => any;
	update:(r:any) => any;
	ex: (ex:Error | any) => any;
}
export class Daemon {
	static conf:any;
	static CGI(basepath: string, conf: any) {
		var domainCache:any = {}; // 执行域缓存
		this.conf = conf;
		function _exec(func: express.RequestHandler,
				req: express.Request,
				res: express.Response,
				next: Function) {
			var key = req.path,
				d: domain.Domain;
			if (!domainCache[key]) {
				d = domainCache[key] = domain.create();
				d.on("error", function(e:Error) {
					res.status(500).send({ error: e.message });
				});
			} else {
				d = domainCache[key];
			}
			d.run(function() {
				if (conf[key]) {
					func.call(conf[key], req, res, next);
				} else {
					func(req, res, next);
				}
			});
		}
		return (<express.RequestHandler>function _CGI(req, res, next) {
			var absolute = path.join(basepath, req.path);
			if (absolute.indexOf(basepath) != 0) return res.status(500).send({ error: "out of jail" });
			absolute = "./" + absolute;
			try {
				var filename = require.resolve(absolute);
			} catch (e) { return res.status(404).send({ error: "module not found" }); }
			var module = require.cache[filename];
			if (module) { return _exec(module.exports, req, res, next); }
			var watcher = fs.watch(filename, { persistent: false });
			watcher.on("change", function() {
				console.log("unload " + filename);
				watcher.close();
				if (module && module.parent) {
					module.parent.children.splice(module.parent.children.indexOf(module), 1);
				}
				delete require.cache[filename];
			});
			_exec(require(absolute), req, res, next);
			console.log("load " + filename);
		});
	};
	_db: Q.Promise<mp.Db>; // 打开的mongodb的promise
	constructor(connection_string: string, username?: string, password?: string) {
		console.log("connect_mongodb(" + arguments ? [].join.apply(arguments) : "" + ")");
		var defer = Q.defer<mp.Db>();
		this._db = defer.promise;
	
		MongoClient.connect(connection_string, {
			native_parser: !!mongodb.BSONNative,
			safe: true
		}).then(function(db:mp.Db) {
			if (username && password) {
				db.authenticate(username, password)
				.then(function(result) {
					console.log("mc.authenticate() => ", result);
					if (result)
						defer.resolve(db);
					else
						defer.reject("username/password");
				}, function(err) {
					console.log("mc.authenticate() error:", err.errmsg);
					defer.reject(err);
				});
			} else {
				console.log("mc.connected()");
				defer.resolve(db);
			}
		}).done();
	};
	session(options: SessionOptions) {
		function _session(opt: SessionOptions) {
			return session({
				secret: opt.sessionSecret,
				resave: true,
				saveUninitialized: true,
				store: new MongoStore(opt),
				cookie: { maxAge: opt.ttl * 1000 } // 会话有效期为30天
			});
		}
		var opt: SessionOptions = {
			db: null,
			ttl: 30 * 24 * 60 * 60,
			touchAfter: 3600, // 每小时自动更新会话一次
			sessionSecret: "session secret",
			stringify: false
		};
		extend(opt, options);
		if (opt.db) { return _session(opt); }
		var stub:express.RequestHandler = null;
		this._db.then(function(db) {
			opt.db = db._db;
			stub = _session(opt);
		});
		return (function() {
			if (stub) { stub.apply(this, [].slice.apply(arguments)); }
		});
	};
	mongodb() { // 向req中注入一些方便方法，并替换res的json方法，支持DBRef展开
		var promise = this._db;
		// 替换express的json响应
		var _json:express.Send = express.Response.json;
		express.Response.json = function _mongodb_json() {
			function replacer(indent: number, path: Array<string|number>, value: any) { // 实际展开值的函数
				//console.log("replacer", indent, path);
				if (typeof value !== "object" || !value) { return value; }
				var con = value.constructor;
				//console.log("replacer", con.name, con === ObjectID);
				var caller = arguments.caller,
					callee = arguments.callee;
				if (Array.isArray(value)) { // Array
					var promises = Array<Q.Promise<any>>();
					(<Array<any>>value).forEach(function(v, i) {
						var sub_path = path.slice();
						sub_path.push(i);
						promises.push(callee(indent, sub_path, v));
					});
					//console.log(promises);
					return Q.all(promises);
				} else if (con === Date) {
					return { $date: value.getTime() };
				} else if (con === ObjectID) { // ObjectID
					return { $id: value.toHexString() };
				} else if (con === DBRef) {
					var defer = Q.defer();
					promise.then(function(db) {
						return db.collection(value.namespace);
					}).then(function(col) {
						var columns = { name: 1 };
						switch (value.namespace) {
						case 'operator':
							columns = { name: 1, id: 1, mobile: 1, phone: 1 };
							break;
						}
						return col.findOne({ _id: value.oid }, columns);
					}).then(function(obj) {
						defer.resolve(obj);
					}).fail(function(err) {
						defer.reject(err);
					});
					return defer.promise;
				} else {
					//console.log("replacer", con.name);
					if (caller === replace) { return value; }
					return replace(indent - 1, path, value);
				}
			};
			function replace(indent: number, path: Array<string|number>, obj: any) { // 决定哪些值应予展开的函数
				//console.log("replace", indent, path);
				if (typeof obj !== "object" || !obj || indent <= 0) { return obj; }
				if (!path) path = [];
				var promises = Array<any>(),
					defer = Q.defer<any>();
				if (Array.isArray(obj)) {
					var indexes = Array<number>(),
						resulta = Array<any>();
					(<Array<any>>obj).forEach(function(v, i) {
						indexes.push(i);
						var sub_path = path.slice();
						sub_path.push(i);
						promises.push(replacer(indent - 1, sub_path, v));
					});
					Q.all(promises).then(function(values) {
						//console.log("replace resolve", keys);
						indexes.forEach(function(index, i) {
							resulta[index] = values[i];
						});
						defer.resolve(resulta);
					}).fail(function(err) {
						defer.reject(err);
					});
				} else {
					var keys = Array<string>(),
						resulto:any = {};
					for (var key in obj) {
						if (!obj.hasOwnProperty(key)) { continue; }
						keys.push(key);
						var sub_path = path.slice();
						sub_path.push(key);
						promises.push(replacer(indent - 1, sub_path, obj[key]));
					}
					Q.all(promises).then(function(values) {
						//console.log("replace resolve", keys);
						keys.forEach(function(key, i) {
							resulto[key] = values[i];
						});
						defer.resolve(resulto);
					}).fail(function(err) {
						defer.reject(err);
					});
				}
				return defer.promise;
			}
			var vo = this, args = Array.prototype.slice.apply(arguments);
			if (args.length == 0) {
				_json.apply(vo, args);
			} else if (typeof args[0] === "object") {
				Q(replace(3, null, args[0])).done(function(value) {
					args[0] = value;
					_json.apply(vo, args);
				});
			} else if (typeof args[1] === "object") {
				Q(replace(3, null, args[1])).done(function(value) {
					args[1] = value;
					_json.apply(vo, args);
				});
			} else {
				_json.apply(vo, args);
			}
		}
		return (<express.RequestHandler>function(req:Request, res:Response, next:Function) {
			// 自动注入某些通用参数（排序、分页等）
			req.col = function collection(col:string) {
				if (typeof col !== "string") throw new Error("need collectionName");
				return promise.then(function(db) { return db.collection(col) });
			};
			req.find = function find(col, query?, fields?, sort?, skip?, limit?) {
				if (typeof col !== "string") throw new Error("need collectionName");
				var $sort = req.query.$sort || req.body.$sort,
					$skip = req.query.$skip || req.body.$skip || 0,
					$limit = req.query.$limit || req.body.$limit || 20,
					$fields = req.query.$fields || req.body.$fields;
				$skip = parseInt($skip);
				$limit = parseInt($limit);
				switch (typeof $sort) {
				case "object":
					break;
				case "string":
					var t = $sort;
					$sort = {};
					$sort[t] = 1;
					break;
				default:
					$sort = { _id: 1 };
				}
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					var cursor = collection.find(query || {}, fields || $fields || {});
					cursor.sort(sort || $sort);
					cursor.skip(skip || $skip);
					cursor.limit(limit || $limit);
					return Q.all([
						cursor.toArray(),
						cursor.count(),
						sort || $sort,
						skip || $skip,
						limit || $limit,
						fields || $fields
					]);
				});
			};
			req._find = res.find = function response_find(r) {
				if (Array.isArray(r) && r.length > 5)
					res.json({
						 $array: r[0],
						 $count: r[1],
						  $sort: r[2],
						  $skip: r[3],
						 $limit: r[4],
						$fields: r[5]
					});
				else if (arguments.length > 5)
					res.json({
						 $array: arguments[0],
						 $count: arguments[1],
						  $sort: arguments[2],
						  $skip: arguments[3],
						 $limit: arguments[4],
						$fields: arguments[5]
					});
				else
					res.json(r);
			};
			req.findOne = function findOne(col, query, fields) {
				if (typeof col !== "string") throw new Error("need collectionName");
				var $fields = req.query.$fields || req.body.$fields;
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					return collection.findOne(query || {}, fields || $fields || {});
				});
			};
			req._array = req._findOne = res.array = res.findOne = function response_one(r) {
				res.json(r);
			};
			req.insert = function insert(col, op) {
				if (typeof col !== "string") throw new Error("need collectionName");
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					return collection.insert(op);
				});
			};
			req._insert = res.insert = function response_insert(r) {
				if (r.result)
					res.json({ insert: r.ops, ok: r.result.ok, n: r.result.n });
				else
					res.json(r);
			};
			req.save = function save(col, op) {
				if (typeof col !== "string") throw new Error("need collectionName");
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					return collection.save(op);
				});
			};
			req.update = function update(col, query, op, options) {
				if (typeof col !== "string") throw new Error("need collectionName");
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					return collection.update(query, op, options);
				});
			};
			req.remove = function remove(col, query) {
				if (typeof col !== "string") throw new Error("need collectionName");
				return promise.then(function(db) { return db.collection(col); })
				.then(function(collection) {
					return collection.remove(query);
				});
			};
			req._remove = req._save = req._update = res.update = function response_update(r) {
				if (r.result)
					res.json({ ok: r.result.ok, nModified: r.result.nModified, n: r.result.n });
				else
					res.json(r);
			};
			// 将参数复制到对象
			req._export = function request_export(data, names) {
				names.forEach(function(name) {
					data[name] = req.query[name] || req.body[name];
				})
			};
			req._exportInt = function request_export_int(data, names) {
				names.forEach(function(name) {
					data[name] = parseInt(req.query[name] || req.body[name]);
				})
			};
			// 匹配输出的方便方法
			req._ex = res.ex = function response_ex(ex) {
				res.status(500).json({ message: ex.message, stack: ex.stack });
			};
			next();
		});
	};
	_moment(exp:any) { // 将输入的参数转化为moment类型的值
		if (/^\d+$/.test(exp)) exp = parseInt(exp);
		exp = moment(exp);
		return exp.isValid() ? exp : null;
	};
	// 混合式验证
	whitelist: string[]; // IP 白名单
	whitelist_add(ip:string) {
		this.whitelist.push(ip);
	};
	basic: string[]; // 用户名密码
	basic_add(username:string, password:string) {
		this.basic.push("Basic " + new Buffer(username + ":" + password).toString("base64"));
	};
	realm = "Authentication Zone";
	hybrid_auth(realm:string) {
		var self = this;
		if (realm) { self.realm = realm; }
		return (<express.RequestHandler>function _hybrid_auth(req, res, next) { // 混合式验证，支持IP白名单和用户名密码
			function _reject() {
				res.set("WWW-Authenticate", "Basic realm=\"" + self.realm + "\"");
				res.sendStatus(401);
			}
			var ip = req.headers["x-real-ip"],
				auth = req.headers["authorization"];
			if (self.whitelist.indexOf(ip) >= 0 // IP 白名单
					|| self.basic.indexOf(auth) >= 0) { // 用户名密码
				next();
			} else if (self._db) {
				var __db: mp.Db;
				self._db.then(function(db) {
					__db = db;
					return __db.collection("hybrid_ip_whitelist");
				}).then(function(col) {
					return col.findOne({ ip: ip });
				}).then(function(record) {
					if (record) {
						next(); // 短路
						return Q.reject(record);
					}
					return __db.collection("hybrid_authorization");
				}).then(function(col:mp.Collection) {
					return col.findOne({ auth: auth });
				}).then(function(record) {
					if (record) {
						next(); // 短路
						return Q.reject(record);
					}
					_reject();
				}).done();
			} else {
				_reject();
			}
		});
	};
}

module.exports = Daemon;