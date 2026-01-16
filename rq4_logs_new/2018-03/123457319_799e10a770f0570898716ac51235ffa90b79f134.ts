import * as mkdirp from 'mkdirp';
import * as pify from 'pify';
import * as vscode from 'vscode';
import * as fs from 'fs';

const mkdirpp = pify(mkdirp);
const statp = pify(fs.stat);

export class QuickMemo {
  constructor() {}

  public async quickMemo(): Promise<void> {
    // Set HomeDirectory
    const homeDir = process.env.HOME;
    const rootDir = `${homeDir}/quick-memo`;
    // make today's directory if needed
    const now = Date.now();
    const date = new Date(now);
    const m = date.getMonth() + 1;
    const d = date.getDate();
    const monthString = m < 10 ? `0${m}` : m;
    const dateString = d < 10 ? `0${d}` : d;

    const dirName = `${rootDir}/${date.getFullYear()}${monthString}${dateString}`;
    await mkdirpp(dirName);
    let input = await vscode.window.showInputBox({
      prompt: 'Input filename'
    });
    // Default file name
    if (input === undefined || input === '') {
      input = `${monthString}${dateString}_${date.getHours()}${date.getMinutes()}${date.getSeconds()}.md`;
    }
    // Set file type as markdown
    if (!input.match(/.md$/)) {
      input = `${input}.md`;
    }

    const filename = input;
    const path = `${dirName}/${filename}`;
    let uri: vscode.Uri;

    // Check file
    let isExist: boolean = true;
    await statp(path).catch(() => {
      isExist = false;
    });

    let doc;
    try {
      if (!isExist) {
        vscode.window.showInformationMessage(`New file created! : ${path}`);
        uri = vscode.Uri.parse(`untitled:${path}`);
      } else {
        uri = vscode.Uri.file(`${path}`);
      }
      doc = await vscode.workspace.openTextDocument(uri);
    } catch (err) {
      vscode.window.showErrorMessage(err);
    }
    if (!doc) {
      return;
    }
    vscode.window.showTextDocument(doc, 1, false);
  }
}