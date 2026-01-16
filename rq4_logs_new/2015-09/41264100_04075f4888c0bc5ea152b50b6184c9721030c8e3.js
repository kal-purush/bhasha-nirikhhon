CRM.Views.UserList = Backbone.View.extend({
	id: "user-list",

	initialize: function () {
		this.collection = new CRM.Collections.Users();
		this.collection.on("sync", this.render, this);
		this.collection.fetch();
	},

	render: function () {
		this.collection.each(function (user) {
			var view = new CRM.Views.User({model: user});
			this.$el.append(view.render().el);
		}, this);
		return this;
	}
});