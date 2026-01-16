import File = require('./filesystem/File');
import ConsoleMessage = require('./userinterface/ConsoleMessage');
import FileFinder = require('./filesystem/FileFinder');

var cm = new ConsoleMessage();

cm.inputMessage("Filename to search: ", (file: string) => {
	var fi = new File(file);

	cm.inputMessage("Local (folder): ",
		(folder: string) => {
			var fo = new File(folder);
			var ff = new FileFinder();

			ff.find(fo, fi, (file: File) => {
				cm.messageLine("found: " + file.getName());
			},
		() => {
			process.exit(0);
		});
	});
});