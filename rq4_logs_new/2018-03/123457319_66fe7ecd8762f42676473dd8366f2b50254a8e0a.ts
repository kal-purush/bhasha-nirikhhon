'use strict';
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import * as mkdirp from 'mkdirp';
import * as pify from 'pify';
import * as fs from 'fs';

const mkdirpp = pify(mkdirp);
const statp = pify(fs.stat);

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
  // Use the console to output diagnostic information (console.log) and errors (console.error)
  // This line of code will only be executed once when your extension is activated
  console.log(
    'Congratulations, your extension "temporary-memo" is now active!'
  );

  // The command has been defined in the package.json file
  // Now provide the implementation of the command with  registerCommand
  // The commandId parameter must match the command field in package.json
  let disposable = vscode.commands.registerCommand(
    'extension.tempMemo',
    async () => {
      // The code you place here will be executed every time your command is executed
      const homeDir = process.env.HOME;
      const rootDir = `${homeDir}/my-test-directory`;
      // make today's directory if needed
      const now = Date.now();
      const date = new Date(now);
      const dirName = `${rootDir}/${date.getFullYear()}${date.getMonth()}${date.getDate()}`;
      await mkdirpp(dirName);
      const filename = 'test-file.md';
      const path = `${dirName}/${filename}`;
      const uri = vscode.Uri.parse(`untitled:${path}`);

      let isExist: boolean = true;
      await statp(path).catch(() => {
        isExist = false;
      });
      const fileuri = vscode.Uri.file(path);
      try {
        if (!isExist) {
          await vscode.workspace.openTextDocument(uri);
          vscode.window.showInformationMessage(`New file created! : ${path}`);
        }
        await vscode.workspace.openTextDocument();
      } catch (err) {
        console.log(err);
        vscode.window.showErrorMessage(err);
      }

      let edit = new vscode.WorkspaceEdit();
      edit.insert(fileuri, new vscode.Position(1, 0), '\n');
      vscode.workspace.applyEdit(edit);
      // wait editing filename
      // if filename is empty then set the date string .md
    }
  );

  context.subscriptions.push(disposable);
}

// this method is called when your extension is deactivated
export function deactivate() {}