var service = {};

service.getRoom = function(req) {
	return req.param('room') || req.param('state');
}

service.getRoomQuery = function(req) {
	var room = service.getRoom(req);
	if (room) {
		return '?room=' + encodeURIComponent(room);
	} else {
		return '';
	}
}

module.exports = service;