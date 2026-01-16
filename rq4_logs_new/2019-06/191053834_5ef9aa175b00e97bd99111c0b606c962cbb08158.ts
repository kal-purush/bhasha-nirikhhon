import fs from 'fs';
import path from 'path';

let rgPath: string | undefined;

// https://github.com/Gruntfuggly/todo-tree/blob/1ed8bd593d2012bf06c47bf878925f95ea998f27/config.js
export function getRgPath(): string {
  if (rgPath) {
    return rgPath;
  }

  rgPath = exePathIsDefined(
    path.join(
      path.dirname(path.dirname(require.main!.filename)),
      'node_modules/vscode-ripgrep/bin/',
      exeName()
    )
  );
  if (rgPath) { return rgPath; }

  rgPath = exePathIsDefined(
    path.join(
      path.dirname(path.dirname(require.main!.filename)),
      'node_modules.asar.unpacked/vscode-ripgrep/bin/',
      exeName()
    )
  );

  if (rgPath) { return rgPath; }

  throw new Error('no rg binary found');
}

function exeName() {
  const isWin = /^win/.test(process.platform);
  return isWin ? 'rg.exe' : 'rg';
}

function exePathIsDefined(rgExePath: string) {
  return fs.existsSync(rgExePath) ? rgExePath : undefined;
}