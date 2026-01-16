var iectrl = require('iectrl');
var vmNames = iectrl.IEVM.names;
var finalExports = {};

vmNames.forEach(function ( vmName ) {
	finalExports['launcher:' + vmName] = ['type', IEVMLauncher];
});

function IEVMLauncher ( id, vmName, baseBrowserDecorator ) {
	baseBrowserDecorator(this);

	this.id = id;
	this.name = vmName;
	this.vm = iectrl.IEVM.find(this.name)[0];
	this.wasRunning = false;
	this.captured = false;

	this.start = function ( url ) {
		var vmUrl;
		var self = this;
		vmUrl = (String(url) + '?id=' + this.id).replace('localhost', iectrl.IEVM.hostIp);
		return this.vm.running().then(function ( running ) {
			self.wasRunning = running;
			if (running) {
				return self.vm.open(vmUrl);
			}
			return self.vm.start(true).then(function () {
				return self.vm.open(vmUrl);
			});
		});
	};

	this.kill = function ( done ) {
		var self = this;
		return this.vm.close().then(function () {
			if ( self.wasRunning ) {
				return done();
			}
			return self.vm.stop().then(function () {
				return done();
			});
		}).catch(done);
	};

	this.forceKill = function () {
		var self = this;
		return self.kill(function () {
			self.emit('done');
		});
	};

	this.markCaptured = function () {
		this.captured = true;
	};

	this.isCaptured = function () {
		return this.captured;
	};
}

IEVMLauncher.$inject = ['id', 'name', 'baseBrowserDecorator'];

module.exports = finalExports;