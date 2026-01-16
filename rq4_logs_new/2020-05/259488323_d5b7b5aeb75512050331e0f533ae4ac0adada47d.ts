/* eslint-disable no-empty-function */
/* eslint-disable no-useless-constructor */

import { IHashTable } from '../interfaces/IHashTable';

export class Parser<T> {
  constructor(
    private dictionary: IHashTable<T>,
    private pattern: RegExp,
  ) { }

  public execute(str: string): Map<T, string> {
    const map: Map<T, string> = new Map();
    for (let match = this.pattern.exec(str); match !== null; match = this.pattern.exec(str)) {
      if (this.dictionary[match[0]]) {
        const regExpArr = this.pattern.exec(str);
        const value = regExpArr?.[0] ?? '';
        map.set(this.dictionary[match[0]], value);
      }
    }
    return map;
  }
}