$(document).ready(function () {
	$("#phone-num-set").click(function () {
		$("#phone-num-setinput").show();
	});
	$("#phone-num-set").click(function () {
		$("#phone-num-set").hide();
	});


	$("#comphone-num-set").click(function () {
		$("#comphone-num-setinput").show();
	});

	$("#comphone-num-set").click(function () {
		$("#comphone-num-set").hide();
	});

	$("#files").change(function () {
		filename = this.files[0].name
		console.log(filename);
	});
});