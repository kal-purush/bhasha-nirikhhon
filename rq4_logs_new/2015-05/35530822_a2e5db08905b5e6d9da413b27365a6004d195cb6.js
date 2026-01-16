var assert = require('assert');
var ms = require('./mingle_slack.js').ms;

ms.createLastUpdateForTest("./testLastUpdated.json");

describe('Mingle-Slack-Integration', function(){
	describe("manupilateMingleData", function(){
		it('should give null for message if entries are empty in mingledata', function(done) {	
			ms.manupilateMingleData({feed:{entry:[]}}, function(message, log) {
				assert.equal(message, null);
				done();
			});
		});

		it('should give a log of "No important events to update" if entries are empty in mingleData', function(done){
			ms.manupilateMingleData({feed:{entry:[]}}, function(message, log) {
				assert.equal(log, 'No important events to update');
				done();
			});
		});

		it('should give a log message if entries updated time is less than lastUpdatedDate', function(done){
			var mingleData = { feed : { entry: [
				{id:["https://testServer/projects/testProject/events.xml"], updated:["2014-05-13T14:53:42Z"]}
			]}};

			ms.manupilateMingleData(mingleData, function(message, log){
				assert.equal(log, 'No important events to update');
				done();
			});
		});

		it('should give a log message if entries are other than card type', function(done) {
			var mingleData = { feed : { entry: [
				{id:["https://testServer/projects/testProject/events.xml"], updated:[new Date().toISOString()], category:[{'$':{term:'page'}}]}
			]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(log, 'No important events to update');
				done();
			});
		});

		it('should give a log message of igoring event if entries otherthan defined activities', function(done) {
			var mingleData = { feed : { updated:[new Date().toISOString()] ,entry: [
				{id:["https://testServer/projects/testProject/events.xml"], title: ["TITLE"], author:[{name:["AUTHOR"]}],updated:[new Date().toISOString()], 
				category:[{'$':{term:'card'}}],
				content:[{changes:[{change:[
					{'$':{type:'description-change'}}
				]}]}]}
			]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(log, 'Ignoring event...(Filtered)');
				done();
			});
		});

		it('should give a log message of updating event if entries contain property-change of name Story Status', function(done) {
			var mingleData = { feed : { updated:[new Date().toISOString()] ,entry: [
				{id:["https://testServer/projects/testProject/events.xml"], title: ["TITLE"], author:[{name:["AUTHOR"]}],updated:[new Date().toISOString()], 
				category:[{'$':{term:'card'}}],
				content:[{changes:[{change:[
					{'$':{type:'property-change'},property_definition:[{name:["Story Status"]}], old_value:["OLD_VALUE"], new_value:['NEW_VALUE']}
				]}]}]}
			]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(log, 'Updating event...');
				done();
			});
		});
		
		it('should give the message with mingle event details if entries contain property-change of name Story Status', function(done) {
			var mingleData = { feed : { updated:[new Date().toISOString()] ,entry: [
				{id:["https://testServer/projects/testProject/events.xml"], title: ["TITLE"], author:[{name:["AUTHOR"]}],updated:[new Date().toISOString()], 
				category:[{'$':{term:'card'}}],
				content:[{changes:[{change:[
					{'$':{type:'property-change'},property_definition:[{name:["Story Status"]}], old_value:["OLD_VALUE"], new_value:['NEW_VALUE']}
				]}]}]}
			]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(message,"--- New Event ---\nTitle: TITLE\n" +
					"Author: AUTHOR\t\tUpdated At: "+ new Date().toString()+"\n" +
					"Story Status changed from OLD_VALUE to NEW_VALUE.\n");
				done();
			});
		});

		it('should give the message with mingle event details if entries contain tag-addition', function(done) {
			var mingleData = { feed : { updated:[new Date().toISOString()] ,entry: [
				{id:["https://testServer/projects/testProject/events.xml"], title: ["TITLE"], author:[{name:["AUTHOR"]}],updated:[new Date().toISOString()], 
					category:[{'$':{term:'card'}}],
					content:[{changes:[{change:[
						{'$':{type:'tag-addition'}, tag:['TAG']}
					]}]}]}
				]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(message,"--- New Event ---\nTitle: TITLE\n" +
					"Author: AUTHOR\t\tUpdated At: "+ new Date().toString()+"\n" +
					"Tag added : TAG");
				done();
			});
		});

		it('should give the message with mingle event details if entries contain tag-addition', function(done) {
			var mingleData = { feed : { updated:[new Date().toISOString()] ,entry: [
				{id:["https://testServer/projects/testProject/events.xml"], title: ["TITLE"], author:[{name:["AUTHOR"]}],updated:[new Date().toISOString()], 
					category:[{'$':{term:'card'}}],
					content:[{changes:[{change:[
						{'$':{type:'tag-deletion'}, tag:['TAG']}
					]}]}]}
				]}};

			ms.manupilateMingleData(mingleData, function(message, log) {
				assert.equal(message,"--- New Event ---\nTitle: TITLE\n" +
					"Author: AUTHOR\t\tUpdated At: "+ new Date().toString()+"\n" +
					"Tag removed : TAG");
				done();
			});
		});
	});
});