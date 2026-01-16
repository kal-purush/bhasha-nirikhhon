function plusbtn(argument) {
	document.cal.result.value += "+";
	document.cal.result.value.style.textAlign="right";

}

function subbtn(argument) {
	document.cal.result.value += "-";
	document.cal.result.value.style.textAlign="right";

}

function mulbtn(argument) {
	document.cal.result.value += "*";
	document.cal.result.value.style.textAlign="right";

}

function divbtn(argument) {
	document.cal.result.value += "/";
	document.cal.result.value.style.textAlign="right";

}

function clrbtn(argument) {
	document.cal.result.value = " ";
	// document.cal.result.value.style.textAlign="right";

}
// function eval(argument) {
// 	"document.cal.result.value";
// 	// body...
// }