import * as vscode from 'vscode';

import { TextDocument, FoldingRange, FoldingRangeList, ProviderResult } from 'vscode';

export default class MagicFoldingProvider implements vscode.FoldingProvider {
    public provideFoldingRanges(document: TextDocument): ProviderResult<FoldingRangeList> {
        return findFoldRanges(document);
    }
}

function findFoldRanges(document: TextDocument) {
    let macroReg = /^#?[A-Z0-9.]+/;
    var folds: FoldingRange[] = []
    var lastLineBlank = false;
    var foldLineStart = null;
    for (var i = 0; i < document.lineCount; i++) {
        let line = document.lineAt(i).text.trim();
        if (!line) {
            if (foldLineStart) {
                folds.push(new FoldingRange(foldLineStart, i-1));
                foldLineStart = null;
            }
            lastLineBlank = true;
            continue;
        } else {
            if (lastLineBlank && !foldLineStart) {
                if (macroReg.test(line)) {
                    foldLineStart = i;
                }
            }
            lastLineBlank = false;
        }
    }
    return new FoldingRangeList(folds);
}