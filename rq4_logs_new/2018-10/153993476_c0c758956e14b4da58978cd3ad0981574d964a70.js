const fs = require('fs');
const path = require('path');
const { app, dialog, nativeImage } = require('electron');

function showMessage(browserWindow) {
	dialog.showMessageBox(browserWindow, {
		type: 'info',
		icon: nativeImage.createFromPath('./anakin.jpg'),
		message: 'Hello',
		detail: 'Just a friendly meow.',
		buttons : ['Meow', 'Close'],
		defaultId: 0
	}, clickedIndex => {
		console.log(clickedIndex);
	});
}

function showOpenDialog(browserWindow) {
	dialog.showOpenDialog(browserWindow, {
		defaultPath: path.join(app.getPath('desktop'), 'memory-info.txt'),
		filters: [
			{ name: 'Text Files', extensions: ['txt'] }
		]
	}, filepaths => {
		if(filepaths) {
			console.log(filepaths, fs.readFileSync(filepaths[0], 'utf8'));
		}
	})
}

function showSaveDialog(browserWindow) {
	dialog.showSaveDialog(browserWindow, {
		defaultPath: path.join(app.getPath('desktop'), 'memory-info.txt')
	}, filename => {
		if(filename) {
			const memInfo = JSON.stringify(process.getProcessMemoryInfo());
			fs.writeFile(filename, memInfo, 'utf8', err => {
				if(err) {
					dialog.showErrorBox('Save Failed.', err.message);
				}
			})
		}
	});
}

module.exports = { showMessage, showOpenDialog, showSaveDialog };