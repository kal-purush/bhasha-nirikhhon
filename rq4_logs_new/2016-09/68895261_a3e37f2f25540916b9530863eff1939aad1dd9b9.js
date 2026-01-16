// complete: Type: Function( jqXHR jqXHR, String textStatus )
// data: Type: PlainObject or String or Array. Data to be sent to the server.
// error: Type: Function( jqXHR jqXHR, String textStatus, String errorThrown )
// headers: Type: PlainObject
// method: Type: String, The HTTP method to use for the request (e.g. "POST", "GET", "PUT").
// success: Type: Function( Anything data, String textStatus, jqXHR jqXHR )
// url: A string containing the URL to which the request is sent.
// async

var $ = {};
$.ajax = function (options) {
	// initialize the XML http request
	var xhr = new XMLHttpRequest();

	// event listener
	xhr.addEventListener("load", options.success);
	xhr.addEventListener("load", options.complete);
	xhr.addEventListener("error", options.error);
	xhr.addEventListener("error", options.complete);

	// main request meat
	xhr.open(options.method, options.url, options.async);

	// header
	xhr.setRequestHeader("Content-type", options.headers);

	// send the request
	xhr.send(options.data);
};
// $.ajax({
// 	url: "http://reqres.in/api/users",
// 	method: "GET",
// 	data: "page=2",
// 	async: true,
// 	headers: "application/x-www-form-urlencoded",
// 	success: function (data) {
// 		console.log(data);
// 	},
// 	error: function () {
// 		console.log("data retrive failed.");
// 	},
// 	complete: function () {
// 		console.log("ajax request complete.");
// 	}
// });


// jQuery.get( url [, data ] [, success ] [, dataType ] )
$.get = function (url, data, success, dataType) {
	// initialize the XML http request
	var xhr = new XMLHttpRequest();

	// event listener
	xhr.addEventListener("load", success);

	xhr.responseType = dataType;

	// main request meat
	xhr.open("get", url, true);

	// send the request
	xhr.send(data);
};

// $.get("http://reqres.in/api/users", "page=2", function (data) {
// 	console.log(data)
// }, "json");

$.post = function (url, data, success, dataType) {
	// initialize the XML http request
	var xhr = new XMLHttpRequest();

	// event listener
	xhr.addEventListener("load", success);

	xhr.responseType = dataType; // ???

	// main request meat
	xhr.open("post", url, true);

	// header
	xhr.setRequestHeader("Content-type", "application/x-www-form-urlencoded");

	// send the request
	xhr.send(data);
};

$.post("http://reqres.in/api/users", "name=morpheus&job=leader", function (data) {
	console.log(data)
}, "json");