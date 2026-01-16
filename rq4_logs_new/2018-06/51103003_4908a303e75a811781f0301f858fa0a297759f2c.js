/**
 * 
 */
var Cohort = function() {
	var TCPMessage = require('./TCPMessage');
	var tms = new TCPMessage('127.0.0.1', 8888, false);  // TCP version JMS
	var me = this;
	
	var callbackFunction = function(proposal) {
		switch(proposal.code) {
			case 'REQUEST': 
				me.request(proposal);
				break;
			case 'DECISIONABORT':
				me.decisionAbort(proposal);
				break;
			case 'DECISIONCOMMIT':
				me.decisionCommit(proposal);
				break;
			case 'CLOSE':
				tms.close();
				break;
		}
	}
	
	this.run = function() {
		tms.subscribe(callbackFunction);
		tms.publish({code : 'CONNECT'});
	}
	
	this.request = function(prop) {
		var v = Math.random();
//		if(v < 0.2) {
//			this.voteNo(prop);
//		} else {
			this.voteYes(prop);
//		}
	}
	
	this.voteNo = function(prop) {
		prop.code = 'ABORT';
		tms.publish(prop);
	}
	
	this.voteYes = function(prop) {
		prop.code = 'COMMIT';
		tms.publish(prop);
	}
	
	this.decisionCommit = function(prop) {
		console.log("Cohort commit");
	}
	
	this.decisionAbort = function(prop) {
		console.log("Cohort abort");
	}
}

//console.log('[Cohort pid] ' + process.pid);
process.on('message', function(msg){
	if(msg == 'init') {
//		console.log('[Init] cohort init.');
		new Cohort().run();
	}
});