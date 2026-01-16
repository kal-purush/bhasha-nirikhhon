Controller('markdownParser', {
	events: {
		'keyup textarea.input': function (e) {

			var rawInput = $("#markdown-parser-container textarea.input").val();

			var rows = rawInput.split('\n');
			var newLines = [];

			rows.forEach( function (line, index) {

				newLines.push(markdownParseLine(line.split(' ')));

			});

			console.log(newLines);
			Session.set('parsedMarkdown', newLines.join('\n'));
		}
	},
	helpers: {
		'parsedMarkdown': function () {
			Session.set('document', document);
			return Session.get('parsedMarkdown');
		}
	}
});

var markdownParseLine = function (line) {

	var replaced = [],
		appended = [];

	line.forEach( function (word, index) {

		markdownChars.forEach( function (mdChar) {
			if ( mdChar.in === word ) {
				console.log('match:', word);

				replaced.push({
					'start': mdChar.out.start,
					'index': index
				});
				appended.push(mdChar.out.end);
			}
		});
	});
	console.log(replaced, appended);

	replaced.forEach( function(pair) {

		var start = pair.start;
		var index = pair.index;

		line[index] = start;
	});

	appended.reverse().forEach( function (ending) {
		line.push(ending);
	});
	console.log(line);
	return line.join(' ');

};

var markdownChars = [

	{
		in: '#',
		out: {
			start: '<h1>',
			end: '</h1>'
		}
	},
	{
		in: '##',
		out: {
			start: '<h2>',
			end: '</h2>'
		}
	},
	{
		in: '###',
		out: {
			start: '<h3>',
			end: '</h3>'
		}
	},
	{
		in: '####',
		out: {
			start: '<h4>',
			end: '</h4>'
		}
	},
	{
		in: '#####',
		out: {
			start: '<h5>',
			end: '</h5>'
		}
	},
	{
		in: '######',
		out: {
			start: '<h6>',
			end: '</h6>'
		}
	},
	{
		in: '*',
		out: {
			start: '<li>',
			end: '</li>'
		},
	},
];