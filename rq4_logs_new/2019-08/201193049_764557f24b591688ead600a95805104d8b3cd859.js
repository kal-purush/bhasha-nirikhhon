const vscode = require('vscode');
/**@param {vscode.ExtensionContext} context*/
function activate(context) {
	var bashName = getlaunchJson("terminal");
	var bashInfo = getlaunchJson("shell");
	var terminal = vscode.window.createTerminal(bashName, "", "");

	let disposable = vscode.commands.registerCommand('launch-command', function () {
		vscode.window.showInformationMessage('running shell ING ....');
		terminal.show();
		terminal.sendText(bashInfo);
	});

	context.subscriptions.push(disposable);
}
exports.activate = activate;

function deactivate() {
}

module.exports = {
	activate,
	deactivate
}

//----custom---func----//
function getlaunchJson(args) {
	var launch_config = vscode.workspace.getConfiguration('launch', vscode.Uri.parse('./.vscode/launch.json'));
	var launch_values = launch_config.get(args);
	return launch_values;
}