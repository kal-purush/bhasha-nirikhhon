var volumeControl = {
	createEvent: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback, // success callback function
            errorCallback, // error callback function
			'VolumeControl',
            'testVolume', // with this action name
           	[{ }] 
		);
	},
	setModeSilent: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback, // success callback function
           	errorCallback, // error callback function
			'VolumeControl',
            'setModeSilent', // with this action name
           	[{ }] 
		);
	},
	setModeVibrate: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback, // success callback function
           	errorCallback, // error callback function
			'VolumeControl',
            'setModeVibrate', // with this action name
           	[{ }] 
		);
	},
	setModeNormal: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback, // success callback function
           	errorCallback, // error callback function
			'VolumeControl',
            'setModeNormal', // with this action name
           	[{ }] 
		);
	},
	getMusicVolume: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback, // success callback function
           	errorCallback, // error callback function
			'VolumeControl',
            'getVolume', // with this action name
           	[{ }] 
		);
	},
	setMusicVolume: function(volume, successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'setVolume', 
           	[{ "volume":volume }] 
		);
	},
	getRingingVolume: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'getRingingVolume', 
           	[{ }] 
		);
	},
	setRingingVolume: function(volume, successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'setRingingVolume', 
           	[{ "volume":volume }] 
		);
	},
	volumeUp: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'volumeUp', 
           	[{ }] 
		);
	},
	volumeDown: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'volumeDown', 
           	[{ }] 
		);
	},
	getDeviceMaxVolume: function(successCallback, errorCallback) {
		cordova.exec(
			successCallback,
           	errorCallback, 
			'VolumeControl',
            'getDeviceMaxVolume', 
           	[{ }] 
		);
	}
	
}		
module.exports = volumeControl;