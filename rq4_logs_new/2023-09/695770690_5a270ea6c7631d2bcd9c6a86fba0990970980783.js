// why are you here this source code is a literal mess
// gl tryna configure anything or add any custom stuff

const vscode = require('vscode');
const config = vscode.workspace.getConfiguration("rconsole")
const showOnStartup = config.get('showOnStartup')
const clearOnDisconnect = config.get('clearOnDisconnect')
let vscConsole = vscode.window.createOutputChannel("RConsole",true);
vscConsole.appendLine("RConsole is active.")

if (showOnStartup) {
	vscConsole.show()
}

/**
 * @param {vscode.ExtensionContext} context
 */
function activate(context) {
	function print(msg) {
		vscConsole.appendLine(msg)
	}
	const WebSocket = require('ws');
	const wss = new WebSocket.Server({ port: 3000 });
	var connected = null;
	wss.on('connection', (ws) => {
		print("client connected!")
		connected = true
		ws.on('message', (message) => {
			if (message == "__RCONSOLE_EXCLUSIVE_FUNCTION.PLAYER_LEFT_GAME") {
				ws.close()
				vscConsole.clear()
				print('client disconnected')
				connected = false
			} else {
				print(`${message}`);
			}
		});
	});

	let disposable = vscode.commands.registerCommand('rconsole.checkConnection', () => {
		console.log('output')
		if (connected) {
			vscode.window.showInformationMessage('RConsole is connected!');
			wss.send('RConsole is connected!')
		} else {
			vscode.window.showInformationMessage('RConsole is not connected. :(');
		}
	});

	context.subscriptions.push(disposable);
}

function deactivate() { }

// eslint-disable-next-line no-undef
module.exports = {
	activate,
	deactivate
}