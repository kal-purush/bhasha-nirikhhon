"use strict";
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from "vscode";
import { Disposable, TextDocument } from "vscode";
import { gitHelper } from "./gitHelper";
import * as _ from "lodash";

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed

async function showCommitScreen() {
  let commitDisplay: TextDocument = await vscode.workspace.openTextDocument({
    content: "Type a commit message!",
    language: "git-commit"
  });

  await vscode.window.showTextDocument(commitDisplay);
}

export function activate(context: vscode.ExtensionContext) {
  let showCommitCmd: Disposable = vscode.commands.registerCommand(
    "bettercommits.showCommitScreen",
    showCommitScreen
  );

  context.subscriptions.push(showCommitCmd);
}

// this method is called when your extension is deactivated
export function deactivate() {}