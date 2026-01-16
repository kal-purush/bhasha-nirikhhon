
import Util = require('asimplia-util');
import DependencyInjection = Util.DependencyInjection;
import mongoose = require('mongoose');
var pg = require('pg');
var neo4j = require('neo4j');

export = ConnectionDispatcher;
class ConnectionDispatcher {

	private connectionListeners = [];
	private neo4jListeners = [];
	private mongooseConnection: mongoose.Mongoose;
	private pgClient;
	private neo4jDatabase;

	static $inject = ['DependencyInjection'];
	constructor(
		private di: DependencyInjection
	) {}

	connectMongoDB(dsn: string, callback?: (mongoose: mongoose.Mongoose) => void) {
		this.mongooseConnection = mongoose.connect(dsn, (e) => {
			if (e) {
				throw e;
			}
			this.di.addService('connection.mongoose', this.mongooseConnection);
			console.log('Connected MongoDB');
			if (typeof callback === 'function') {
				callback(this.mongooseConnection);
			}
		});
	}

	connectPostgres(connectionString: string, callback?: (client: any) => void) {
		var schema = 'public'; // TODO
		var client = new pg.Client(connectionString);
		client.connect((e) => {
			if (e) {
				throw e;
				return;
			}
			this.di.addService('connection.postgres', this.mongooseConnection);
			console.log('Connected Postgres');
			this.pgClient = client;
			this.connectionListeners.forEach((callback) => {
				callback(this.pgClient);
			});
			if (typeof callback === 'function') {
				callback(this.pgClient);
			}
		});
		client.query('SET search_path TO '+schema+';');
	}

	connectNeo4j(dsn: string, callback?: (db: any) => void) {
		var db = new neo4j.GraphDatabase(dsn);
		db.query('RETURN 1;', {}, (e: Error, res: any) => {
			if (e) {
				throw e;
			}
			this.di.addService('connection.neo4j', this.neo4jDatabase);
			console.log('Connected Neo4j');
			this.neo4jDatabase = db;
			this.neo4jListeners.forEach((callback) => {
				callback(this.neo4jDatabase);
			});
			if (typeof callback === 'function') {
				callback(this.neo4jDatabase);
			}
		});
	}

	/** @deprecated Use DI $inject */
	getConnection(callback: (connection: any) => void) {
		this.connectionListeners.push(callback);
		if (this.pgClient) {
			callback(this.pgClient);
		}
	}

	/** @deprecated Use DI $inject */
	getGraphDatabase(callback: (db: any) => void) {
		this.neo4jListeners.push(callback);
		if (this.neo4jDatabase) {
			callback(this.neo4jDatabase);
		}
	}

	/** @deprecated Use DI $inject */
	getMongooseConnection(callback: (mongoose: mongoose.Mongoose) => void) {
		callback(this.mongooseConnection);
	}
}