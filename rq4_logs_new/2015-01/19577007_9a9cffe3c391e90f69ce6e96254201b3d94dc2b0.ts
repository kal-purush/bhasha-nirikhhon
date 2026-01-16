
import mongoose = require('mongoose');
import Repository = require('../src/index');

export = create;
var create = (beforeEach: Function, afterEach: Function) => {
	return new Integration(beforeEach, afterEach);
};

class Integration {

	constructor(beforeEach: Function, afterEach: Function) {
		beforeEach((done) => {
			Repository.connectMongoDB('mongodb://localhost:27017/farfalia_test', () => {
				done();
			});
		});
	}

	setup(beforeEach: Function, afterEach: Function) {
		beforeEach((done) => {
			mongoose.connection.db.executeDbCommand({ dropDatabase: 1 }, (e, res) => {
				if (e) {
					console.error(e);
					process.exit(1);
					return;
				}
				console.log('MongoDB dropped');
				done();
			});
		});
		afterEach(() => {
			
		});
	}
	
}