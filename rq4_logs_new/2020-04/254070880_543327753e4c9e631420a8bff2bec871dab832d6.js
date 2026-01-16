module.exports = {
	initSecureCache : function(success, error) {
		cordova.exec(success, error, "SecureCache", "initSecureCache", []);
	}
	
};