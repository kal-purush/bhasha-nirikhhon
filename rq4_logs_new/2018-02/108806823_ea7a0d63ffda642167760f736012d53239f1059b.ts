import * as path from 'path';

import { IMPORT_REGEX, IMPORT_REPLACE_REGEX } from './config';

export const mapKeyRegex = (key: string): RegExp => new RegExp(key.replace('/*', ''));

export const keysRegex = (keys: string[]): RegExp[] => keys.map(mapKeyRegex);

export const matchRegex = (regexes: RegExp[], text: string): boolean =>
  regexes.some(someMatchRegex(text));
export const someMatchRegex = (text: string) => (regex: RegExp): boolean => regex.test(text);

export const importPath = (importText: string): string | undefined => {
  const execs = IMPORT_REGEX.exec(importText);
  if (execs) {
    return execs[2];
  }
  return undefined;
};

export const replaceImportPath = (importText: string, source: string, dest: string): string =>
  importText.replace(IMPORT_REPLACE_REGEX, `$1${path.relative(source, dest)}$3`);

export const mapLine = (pathsRegex: RegExp[], matchPath: any, file: string) => (
  line: string,
): string => {
  const pat = importPath(line);
  if (pat) {
    if (matchRegex(pathsRegex, pat)) {
      const result = matchPath(pat);
      if (result) {
        return `${replaceImportPath(line, path.dirname(file), result)}\n`;
      }
    }
  }
  return line;
};
export const newImportLines = (lines: string[], ...rest: any[]): string[] =>
  lines.map(mapLine(rest[0], rest[1], rest[2]));