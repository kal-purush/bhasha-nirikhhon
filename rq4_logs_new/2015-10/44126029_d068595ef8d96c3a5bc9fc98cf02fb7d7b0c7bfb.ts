import $ = require("jquery");
import UserView = require("view/UserView");
import UserModel = require("model/UserModel");

function app() {
	$("body").append(new UserView({model : new UserModel()}).render().$el);
	console.log("shit don");
}

export = app;