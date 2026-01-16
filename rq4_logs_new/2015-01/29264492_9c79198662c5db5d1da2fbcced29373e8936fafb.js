var Browser = require("zombie");

var browser = new Browser({ debug: true, runScripts: false });

var changePassword = function(email, password, newPassword, successCallback, errorCallback) {
	browser.visit("https://www.linkedin.com/uas/login");
		.done(function() {
		browser
			.fill("session_key", email)
			.fill("session_password", password)
			.pressButton("signin")
			.done(function() {
				var error = browser.query(".error strong");
				if (error) {
					errorCallback([error.innerHTML]);
					return;
				}
				
				browser
					.visit("https://www.linkedin.com/uas/change-password?goback=%2Enas_*1_*1_*1_*1_*1", function() {
						browser
							.fill("oldPassword", password)
							.fill("new_password", password)
							.fill("new_password_again", newPassword)
							.pressButton('submit')
							.done(function() {
								var error = browser.query(".error strong");
								
								if (error) {
									var errorsResult = [];
									errorsResult.push(error.innerHTML);
									
									var errors = browser.queryAll("span.error");
									if (errors) {
										for (var i in errors) {
											errorsResult.push(errors[i].innerHTML);
										}
									}
									
									errorCallback(errorsResult);
								} else {
									successCallback("Password successfully changed!");
								}
							});
					});
			});
	});
}

module.exports.changePassword = changePassword;