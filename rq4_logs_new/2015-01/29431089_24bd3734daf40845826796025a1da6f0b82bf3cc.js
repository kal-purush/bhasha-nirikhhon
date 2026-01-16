//Sample code to read in test cases:
var fs  = require("fs");
var lPosition = -1;

function formatLine(lPosition, lGate){
	if(lPosition == -1 || lPosition == lGate)
		return "|";
	else if(lPosition < lGate)
		return "\\";
	else if(lPosition > lGate)
		return "/";
	return "";
}

function replaceAt(lIndex, lReplace, lLine){
	return lLine.substr(0, lIndex) + lReplace + lLine.substr(lIndex + 1);
}

fs.readFileSync(process.argv[2]).toString().split('\n').forEach(function (line) {
	if (line != "") {
		var lCheckpoint = line.indexOf("C");
		if(lCheckpoint < 0){
			lCheckpoint = line.indexOf("_");
		}
		console.log(replaceAt(lCheckpoint, formatLine(lPosition, lCheckpoint), line));
		lPosition = lCheckpoint;
	}
});