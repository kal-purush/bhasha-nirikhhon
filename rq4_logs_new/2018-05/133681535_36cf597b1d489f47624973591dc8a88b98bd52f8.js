({
	initialize : function(component, event, helper) {
		console.log('Markdown initialized');
		window.component = component;
		window.event = event;
	},

	onload: function(component, event, helper) {
		
	},
	recordUpdated: function(component, event, helper) {
		console.log('Test');
	}
})