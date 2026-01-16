define(['app'], function(app) {

	var lastTimestamp;

	var module = {
		id : 'chat',
		order : 1,
		label : 'Chat',
		interval : 1000,
		data : {
			chat : {},
			chatlines : 'Loading... Please wait',
		}
	};

	module.controller = ['$scope', 'moduleManager',
	function($scope, moduleManager) {
		$scope.data = module.data;
		module.scope = $scope;
		lastTimestamp = null;
		this.sendMessage = function(msg) {
			moduleManager.post('module/chat/data', {
				message : msg,
			});
		};
	}];

	module.update = function(data) {
		angular.merge(module.data.chat, data.chat);

		module.data.chatlines = '';
		forEachSorted(module.data.chat, function(e) {
			module.data.chatlines += e + '<br>';
		});

		var el = document.getElementById("chatlog");
		el.scrollTop = el.scrollHeight;
	};

	app.registerModule(module);

	function forEachSorted(obj, worker) {
		var keys = [];
		for (var key in obj) {
			if (obj.hasOwnProperty(key))
				keys.push(key);
		}
		keys.sort();
		for (var i = 0; i < keys.length; i++) {
			worker(obj[keys[i]]);
		}
	};

});