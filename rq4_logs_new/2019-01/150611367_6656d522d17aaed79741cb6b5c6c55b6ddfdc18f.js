const _ = require('lodash');
const util = require('../lib/util');
const fs = require('fs');
var globule = require('globule');
var path = "google-href-lang/result/international-targeting/*.csv";

async function process() {
	let files = globule.find(path);
	for (var i = 0; i < files.length; i++) {
		let file = files[i];
		console.log("Processing file", file);
		await processItem(file);
	}
}

function processItem(file) {
	return new Promise(resolve => {
		let fileName = file.split("/");
		fileName = fileName[fileName.length - 1];
		fs.readFile(file, 'utf8', async function (err, data) {
			var json = util.CSVToArray(data);
			json = await util.buildInternationalArr(json);
			var lineArray = [];
			json.forEach(function (infoArray, index) {
				var line = "";
				infoArray.forEach((item, i) => {
					if (i === 3 && index !== 0) {
						line += ",\"" + item + "\"";
					} else {
						line += line ? "," + item : item;
					}
				});
				lineArray.push(line);
			});
			var csvContent = lineArray.join("\n");
			fs.writeFile("google-href-lang/results/international-targeting/" + fileName, csvContent, 'utf8', function (err) {
				resolve();
				if (err) {
					console.log('Some error occured - file either not saved or corrupted file saved.', err);
				} else {
					console.log('It\'s saved!');
				}
			})
		});
	});

}

process();