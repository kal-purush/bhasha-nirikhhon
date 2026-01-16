Package.describe({
	name: "velocity:test-proxy",
	summary: "Dynamically created package to expose test files to mirrors",
	version: "0.0.4",
	debugOnly: true
});

Package.onUse(function (api) {
	api.use("coffeescript", ["client", "server"]);
	api.add_files("tests/mocha/client/sampleClientTest.js",["client"]);
	api.add_files("tests/mocha/server/admin/startup.js",["server"]);
	api.add_files("tests/mocha/server/admin/user.js",["server"]);
	api.add_files("tests/mocha/server/checkRights/checkUserRight.js",["server"]);
	api.add_files("tests/mocha/server/checkRights/makeAndRemoveAdmin.js",["server"]);
});