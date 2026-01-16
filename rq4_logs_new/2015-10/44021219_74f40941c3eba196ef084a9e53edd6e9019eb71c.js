var
	blueprintCreate = require("../../node_modules/sails/lib/hooks/blueprints/actions/create"),
	actionUtil = require('sails/lib/hooks/blueprints/actionUtil'),
	request = require('superagent');

module.exports = function(req, res) {
	var
		Model = actionUtil.parseModel(req),
		data = actionUtil.parseValues(req),
		modelName = Model.adapter.collection;
	if (modelName == 'places' || modelName == 'rooms') {
		request
			.post('https://manager.gimbal.com/api/beacons/')
			.send({
				factory_id: data.beaconFactoryID,
				name: data.name,
				visibility: 'public'
			})
			.set('Authorization', 'Token token=937abb7296a1a28e8e88329b0b9b9067')
			.end(function (err, resp) {
				var beaconID = resp.body.id;
				request
					.post('https://manager.gimbal.com/api/v2/places/')
					.send({
						name: data.name,
						beacons: [{
							id: beaconID,
							name: data.name
						}]
					})
					.set('Authorization', 'Token token=937abb7296a1a28e8e88329b0b9b9067');
			});
	}
	return blueprintCreate(req, res);
};