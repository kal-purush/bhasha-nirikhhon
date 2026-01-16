import * as vscode from 'vscode';

export class Editor {
  static async open() {
    const textDocument: vscode.TextDocument = await vscode.workspace.openTextDocument(/*{ language: Consts.languageId }*/);
    const textEditor: vscode.TextEditor = await vscode.window.showTextDocument(
      textDocument,
      { preview: false }
    );

    textEditor.edit(edit => {
      const pos = new vscode.Position(0, 0);
      edit.insert(pos, `test\n\n\n\ntest\n\n\ntest\ntest`);
      textEditor.document.save();
    });
  }
}