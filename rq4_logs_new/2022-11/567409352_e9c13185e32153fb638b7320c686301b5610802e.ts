import * as vscode from 'vscode';

export function activate(context: vscode.ExtensionContext) {
	console.log('Congratulations, your extension "ciao-prolog" is now active!');

	const run = vscode.commands.registerCommand('ciao-prolog.run', () => {
		vscode.window.showInformationMessage('Runing with ciao');
		const t: vscode.Terminal = vscode.window.createTerminal('ciao-shell');
		t.show();
		t.sendText('ciao\n');
		const activeFile: String | undefined = vscode.window.activeTextEditor?.document.uri.fsPath;
		if (activeFile) {
			t.sendText(`use_module('${vscode.window.activeTextEditor?.document.uri.fsPath}').\n`);
		}
	});

	context.subscriptions.push(run);
}

export function deactivate() {}