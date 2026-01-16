'use strict';

import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';

import * as nls from 'vscode-nls';

// var localize = nls.loadMessageBundle(__filename);
let localize = nls.loadMessageBundle();

interface MyMessageItem extends vscode.MessageItem {
	id: number;
}

var execPath = process.execPath;
var pathname = path.dirname(execPath).split("/");
var webviewPreJsPath = path.join(path.parse(execPath).root, pathname[0], pathname[1], pathname[2], pathname[3], '/Resources/app/out/vs/workbench/parts/html/browser/webview-pre.js');

export function activate(context: vscode.ExtensionContext) {

    console.log('Congratulations, your extension "vscode-findinpage" is now active!');

    let disposable = vscode.commands.registerCommand('extension.findinpage', () => {

		const media = context.asAbsolutePath(path.join('media', 'webview-pre_findinpage.js'));
		webviewPreJsToggle(media);

	});

    context.subscriptions.push(disposable);
}

function webviewPreJsToggle(media) {
	let fileBeing;
  if(!statPath(webviewPreJsPath + '.orig')) {
		// console.log('file/dir not found: ' + webviewPreJsPath + '.orig');
		fileBeing = "Disabled";
	} else {
		// console.log('file/dir exists: ' + webviewPreJsPath + '.orig');
		fileBeing = "Enabled"; 
	}
	vscode.window.showInformationMessage<MyMessageItem>(
				localize('toggle', 'Would you like to Enable the "Find in page" in the preview editor? (Currently {0})', fileBeing),
				{
					title: localize('apply', 'Apply'),
					id: 1
				},
				{
					title: localize('restore', 'Restore'),
					id: 2
				},
				{
					title: localize('close', 'Close'),
					id: 3,
					isCloseAffordance: true
				}
			).then((selected) => {
				// console.log(selected);
				if (!selected || selected.id === 3) {
					// console.log("case 3");
					return;
				}
				switch (selected.id) {
					case 1:
						// console.log("case 1");
						if (fileBeing === "Enabled"){
							vscode.window.showInformationMessage('Please with peace of mind. It has already been applied.');
							break;
						} else {
							// console.log(webviewPreJsPath + '.orig is not exist');
							fs.writeFileSync(webviewPreJsPath + '.orig', fs.readFileSync(webviewPreJsPath,"utf-8"), 'utf8');
							fs.writeFileSync(webviewPreJsPath, fs.readFileSync(media,"utf-8"), 'utf8');
							vscode.window.showInformationMessage('Apply to "Find in Page" in Preview Editor has been completed');
						}
						break;
					case 2:
						// console.log("case 2");
						if (fileBeing === "Enabled"){
							// console.log(webviewPreJsPath + '.orig is  exists.');
							fs.writeFileSync(webviewPreJsPath, fs.readFileSync(webviewPreJsPath + '.orig', "utf-8"), 'utf8');
							fs.unlinkSync(webviewPreJsPath + '.orig');
							vscode.window.showInformationMessage('Restore is complete.');
							break;
						} else {
							vscode.window.showInformationMessage('No "Find in Page" in Preview Editor has been applied.');
						}
				}
			});
			return ;
}

function statPath(path) {
  try {
    return fs.statSync(path);
  } catch (ex) {}
  return false;
}