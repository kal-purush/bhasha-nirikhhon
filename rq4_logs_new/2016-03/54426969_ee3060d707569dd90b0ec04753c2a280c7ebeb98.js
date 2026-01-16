Template.bCard.helpers({

	firstName: function() {
		return Session.get('firstName');
	},

	lastName: function() {
		return Session.get('lastName');
	},

	quote: function() {
		return Session.get('quote');
	},

	gender: function() {
		return Session.get('gender');
	},

	githubLink: function() {
		return Session.get('githubLink');
	}
});