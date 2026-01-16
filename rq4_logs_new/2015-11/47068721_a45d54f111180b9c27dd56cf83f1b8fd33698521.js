
define([
	'./views/layout/mainNav/MainNav',
	'./views/layout/statisticNav/StatisticNav',
	'./views/layout/operatorInfoTable/OperatorInfoTable',
	'./views/layout/operatorData/OperatorData',
	'./views/layout/operatorForm/OperatorForm',
],

function (MainNav, StatisticNav, OperatorInfoTable, OperatorData, OperatorForm) {

	var app = window.app || {};		

	window.app = app;
	app.vent = _.extend({}, Backbone.Events);	
	
	app.initialize = function() {

		this.mainNav = new MainNav();
		this.statisticNav = new StatisticNav();
		this.operatorInfoTable = new OperatorInfoTable();
		this.operatorData = new OperatorData();
		this.peratorForm = new OperatorForm();
		 
	};
	return app;
});
