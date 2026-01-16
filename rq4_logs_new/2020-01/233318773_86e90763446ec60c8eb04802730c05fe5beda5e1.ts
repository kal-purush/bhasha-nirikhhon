import { Int } from "../int.d";

export default class GetMacAddress {
  constructor() {}

  getMACAddress(): string {
    return '00:00:00:00:00:00';
  }

  getMACAddressHashed(): Int {
    return this.hashCode(this.getMACAddress());
  }

  hashCode(word: string): Int {
    var hash = 0, i, chr;
    if (word.length === 0) return hash as Int;
    for (i = 0; i < word.length; i++) {
      chr   = word.charCodeAt(i);
      hash  = ((hash << 5) - hash) + chr;
      hash |= 0; // Convert to 32bit integer
    }
    return hash as Int;
  };
}