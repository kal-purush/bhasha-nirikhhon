import { Rule } from './Rule';
import { ParsedPath } from 'path';
import * as path from 'path';

export class Match {
  parsedPath: ParsedPath;
  rule: Rule | undefined;
  possibleMatches: string[] = [];

  constructor(parsedPath: ParsedPath, possibleMatches: string[], rule?: Rule) {
    this.parsedPath = parsedPath;
    this.rule = rule;
    this.possibleMatches = possibleMatches;
  }

  hasRule(): boolean {
    return !!this.rule;
  }

  isTest(): boolean {
    return !this.rule?.isTest;
  }

  getFileName(): string {
    return path.join(this.parsedPath.dir, this.parsedPath.base);
  }
}