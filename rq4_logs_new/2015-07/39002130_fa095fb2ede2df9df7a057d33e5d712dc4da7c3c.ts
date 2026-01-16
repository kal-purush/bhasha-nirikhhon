/// <reference path="typings/express/express.d.ts" />
/// <reference path="typings/q/Q.d.ts" />
/// <reference path="typings/mongodb/mongodb.d.ts" />
/// <reference path="typings/mongodb-promise/mongodb-promise.d.ts" />
/// <reference path="typings/moment/moment.d.ts" />
declare module "Daemon" {
	import express = require("express");
	import Q = require("q");
	import mongodb = require("mongodb");
	import mp = require("mongodb-promise");
	module Daemon {
		interface SessionOptions {
			db: mongodb.Db;
			ttl: number;
			touchAfter: number; // 自动更新会话
			sessionSecret: string; // 会话密钥
			stringify: boolean;
		}
	
		interface UpdateResult {
			result: {
				ok:number,
				n: number,
				nInserted: number
				nMatched: number
				nModified: number
				nRemoved: number
				nUpserted: number
			}
		}
		interface InsertResult extends UpdateResult {
			ops: {}
		}
		interface Request extends express.Request {
			col:<T>(collectionName:string) => Q.Promise<mp.Collection<T>>;
			find:(col:string, query?:{}, fields?:{}, sort?:{}, skip?:number, limit?:number) => Q.Promise<any[]>;
			_find:<T>(array:T[], count?:number, sort?:{}, skip?:number, limit?:number, fields?:{}) => void;
			findOne:<T>(col:string, query:any, fields?:any) => Q.Promise<T>;
			_findOne:<T>(r:T) => void;
			_array:<T>(r:T[]) => void;
			insert:(col:string, op:any) => Q.Promise<InsertResult>;
			_insert:(r:InsertResult) => void;
			save:<T>(col:string, op:T) => Q.Promise<UpdateResult>;
			_save:<T>(r:UpdateResult) => void;
			update:(col:string, query:any, op:any, options?:any) => Q.Promise<UpdateResult>;
			_update:(r:UpdateResult) => void;
			remove:(col:string, op:any) => Q.Promise<UpdateResult>;
			_remove:(r:UpdateResult) => void;
			_ex: (ex:Error | {}) => void;
		
			_export:(data:any, name:string[]) => void;
			_exportInt:(data:any, name:string[]) => void;
		}
		interface Response extends express.Response {
			find:<T>(array:T[], count?:number, sort?:{}, skip?:number, limit?:number, fields?:{}) => void;
			findOne:<T>(r:T) => void;
			array:<T>(r:T[]) => void;
			insert:(r:InsertResult) => void;
			update:(r:UpdateResult) => void;
			ex: (ex:Error | any) => void;
		}
		interface Route {
			(req: Request, res: Response, ...data:any[]): void;
		}
		function r(route: Route): Route
		function CGI(path: string, conf?: {})
	}
	
	class Daemon {
		constructor(url:string)
		_moment(exp:string|number): moment.Moment
		collection<T>(col:string): Q.Promise<mp.Collection<T>>
		session(options: Daemon.SessionOptions): express.RequestHandler
		mongodb(): express.RequestHandler
		hybrid_auth(): express.RequestHandler
		whitelist_add(ip:string): void
		basic_add(ip:string): void
		realm: string
	}
	export = Daemon
}