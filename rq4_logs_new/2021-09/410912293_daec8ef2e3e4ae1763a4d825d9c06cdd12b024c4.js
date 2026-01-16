// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
const vscode = require('vscode');

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed

/**
 * @param {vscode.ExtensionContext} context
 */
function activate(context) {

	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	console.log('Congratulations, your extension "krita-rmd" is now active!');

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with  registerCommand
	// The commandId parameter must match the command field in package.json
	let disposable = vscode.commands.registerCommand('krita-rmd.createFigure', async function () {
		// The code you place here will be executed every time your command is executed

		if (vscode.window.activeTextEditor === undefined) {
			return;
		}

		const searchQuery = await vscode.window.showInputBox({
			placeHolder: "Search query",
			prompt: "Search my snippets on Codever",
		  });

		if (searchQuery === '' || searchQuery === undefined){
			return;
		}

		const wsPath = vscode.window.activeTextEditor.document.uri.fsPath // path to file in active editor

		if (!wsPath.includes("/") && !wsPath.includes("\\") ) { // check if file is unsaved
			return;
		}

		const test = wsPath + '/../figures/' + searchQuery + '.kra'
		const filePath = vscode.Uri.file(test); // path of image

		const templatePath = vscode.Uri.file('C:/Users/rag/Pictures/Template.kra') // path to template

		vscode.workspace.fs.copy(templatePath, filePath, { overwrite: false }) // copy template to image file

		vscode.window.showInformationMessage('Created ' + searchQuery + '.kra');
		console.log('Created ' + filePath);

		vscode.window.activeTextEditor.edit((editBuilder) =>
			editBuilder.insert(
				vscode.window.activeTextEditor.selection.active,
				'```{r ${searchQuery}, echo = FALSE, fig.cap = "", fig.align="center"} \n' +
				'knitr::include_graphics("figures/${searchQuery}.png") \n' +
				'```'
			)
		);

		const terminal = vscode.window.createTerminal();
		terminal.sendText('"C:/Program Files/Krita (x64)/bin/krita.exe" "' + test + '" & exit');
		// terminal.dispose();
	});

	context.subscriptions.push(disposable);
}

// this method is called when your extension is deactivated
function deactivate() {}

module.exports = {
	activate,
	deactivate
}