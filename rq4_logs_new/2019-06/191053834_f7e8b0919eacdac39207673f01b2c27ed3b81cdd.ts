import path from 'path';
import * as vscode from 'vscode';
import lineColumn from 'line-column';

// https://github.com/microsoft/vscode/blob/81d7885dc2e9dc617e1522697a2966bc4025a45d/extensions/markdown-language-features/src/features/documentLinkProvider.ts
// https://github.com/microsoft/vscode/blob/c1c3e5eab0f2fb9e04a32b4fc6473023a9c25697/extensions/markdown-language-features/src/commands/openDocumentLink.ts
// https://github.com/microsoft/vscode/blob/aa575b1e8a3d83df2abd61753077f9d33b9d3c0d/extensions/vscode-api-tests/src/singlefolder-tests/languages.test.ts
export class MarkdownLinkProvider implements vscode.DocumentLinkProvider {
  provideDocumentLinks(
    document: vscode.TextDocument,
    token: vscode.CancellationToken
  ): vscode.ProviderResult<vscode.DocumentLink[]> {
    const source = document.getText();
    const liner = lineColumn(source, {
      origin: 0
    });
    const regEx = /\/.*\.md/g;

    const result: vscode.DocumentLink[] = [];

    let match: RegExpExecArray | null;
    while (match = regEx.exec(source)) {
      const s = liner.fromIndex(match.index);
      const e = liner.fromIndex(regEx.lastIndex);
      const root = vscode.workspace.getWorkspaceFolder(document.uri);
      const p = path.resolve(root!.uri.fsPath, match[0].substring(1));
		  // const x = vscode.Uri.parse(`command:extension.helloWorld?${encodeURIComponent(JSON.stringify({ path: encodeURIComponent(p) }))}`);
      const x = vscode.Uri.parse(`command:_markdown.openDocumentLink?${encodeURIComponent(JSON.stringify({ path: encodeURIComponent(p)}))}`);
      // const x = vscode.Uri.parse(`command:vscode.open?${encodeURIComponent(JSON.stringify({ resource: vscode.Uri.file(p)}))}`);

      result.push(new vscode.DocumentLink(
        new vscode.Range(
          new vscode.Position(s.line, s.col),
          new vscode.Position(e.line, e.col),
        ),
        x
      ));
    }

    console.log('result', result);
    return result;
  }
}