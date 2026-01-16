'use strict';

import * as vscode from 'vscode';
import * as uuid from 'node-uuid';

export function activate(context: vscode.ExtensionContext) {
	let disposable = vscode.commands.registerCommand('extension.guid', () => {

        let textEditor = vscode.window.activeTextEditor;

        vscode.window.activeTextEditor.edit(builder => {
            textEditor.selections.forEach(sel => {
                let result = uuid.v4();
                builder.replace(sel, result.toUpperCase());
            })
        });
	});

	context.subscriptions.push(disposable);
}

export function deactivate() {
}