var cfgApi = require('./pk-lib-cfgapi');
var fs = require('fs-extra');
var path = require('path');

var pkRoot = process.env.PKROOT || __dirname;

cfgApi.readCfg('app1', 'settings', function(data) {
	console.log('read app1 data', data);

	cfgApi.writeCfg('app1', 'settings', {
		a: 1,
		b: 2
	}, function(data) {
		console.log('write app1 data', data);

		cfgApi.readCfg('app1', 'settings', function(data) {
			console.log('read after write app1 data', data);

			cfgApi.writeCfg('app1', 'settings', {
				a: 10,
				b: 20
			}, function(data) {
				console.log('write app1 data', data);

				cfgApi.readCfg('app1', 'settings', function(data) {
					console.log('read after 2nd write app1 data', data);
					var appCfgPath = path.join(pkRoot, 'cfgDb', 'app1', 'settings' + '.json');
					fs.removeSync(appCfgPath);
					cfgApi.readCfg('app1', 'settings', function(data) {
						console.log('read after rm app1 data', data);
					});
				});
			});
		});
	});
});

/*
var content;
content = cfgApi.readCfgSync('app2', 'settings');
console.log('read app2 data', content);
cfgApi.writeCfgSync('app2','settings', {a:1, b:2});
console.log('read after write app2 data', cfgApi.readCfgSync('app2', 'settings'));
cfgApi.writeCfgSync('app2','settings', {a:10, b:20});
console.log('read after write app2 data', cfgApi.readCfgSync('app2', 'settings'));
var appCfgPath = path.join(pkRoot, 'cfgDb', 'app2', 'settings' + '.json');
fs.removeSync(appCfgPath);
console.log('read after rm app2 data', cfgApi.readCfgSync('app2', 'settings'));

*/