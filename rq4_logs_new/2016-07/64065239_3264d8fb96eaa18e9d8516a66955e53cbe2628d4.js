const tsTranslate = require("plugin-typescript").translate,
	ngAnnotate = require("ng-annotate");

function translate(load) {
	return tsTranslate(load).then(function (compiled) {
		const annotated = ngAnnotate(compiled, {add: true});
		load.source = annotated.src;
		return load;
	});
}

exports.translate = translate;