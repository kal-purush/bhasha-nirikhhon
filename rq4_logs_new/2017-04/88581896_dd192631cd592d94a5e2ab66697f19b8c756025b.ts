/**
 * Created by fabalcu97 on 13/04/17.
 */

import {ExpressRouter} from '../../core/classes/ExpressRouter';
import {connectDatabase, MongoModel} from '../../core/classes/MongoModel';
import config from '../../settings/index';
import * as dbModels from '../../core/db-models/models'

let apiRoutes: ExpressRouter;

apiRoutes = new ExpressRouter();

apiRoutes.addRoute('POST', '/addperson', (req, res) => {

	connectDatabase(config.dbConfig.url).then(() => {
		let personModel: MongoModel = new MongoModel('Person');
		let person: dbModels.Person;
		person = req.body.person;
		personModel.insertOne(person).then( (data) => {
			res.status(200);
			res.send(data);
			res.end();
		}).catch((err) => {
			res.send(err);
			res.end();
		});

	}).catch((err) => {
		console.log(err);
	});
});

export default apiRoutes;
