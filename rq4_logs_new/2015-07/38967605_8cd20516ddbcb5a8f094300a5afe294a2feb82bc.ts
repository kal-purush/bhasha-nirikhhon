import {HEX_REGEX} from './constants/hexadecimal';
import {EMPTY_STRING} from './constants/string';

export class Hexadecimal {

  private data: Uint8Array = null;
  private strValue: string = EMPTY_STRING;


  public get value(): Uint8Array {
    return this.data;
  }


  constructor(value: string|Uint8Array) {

    if(typeof value === 'object' && value instanceof Uint8Array) {
      this.data = value;
    } else if(typeof value === 'string'){
      if(Hexadecimal.isValid(value)) {
        this.parseHexadecimal(value);
      } else {
        throw 'Invalid Hexadecimal value provided: ' + value;
      }
    }
  }


  private parseHexadecimal(nibbles: string) {
    var len = nibbles.length;
    var length = len / 2;
    var array = new Uint8Array(length);
    var index = 0;

    for(let i = 0; i < nibbles.length; i += 2) {
      let octet = nibbles[i] + nibbles[i + 1];
      array.BYTES_PER_ELEMENT = 1;
      array[index] = parseInt(octet, 16);
      index += 1;
    }
    this.data = array;
  }


  public static isValid(value: string): boolean {
    if(typeof value === 'string') {
      let isHex: boolean = true;
      for(let letter of value) {
        if(!letter.match(HEX_REGEX)) {
          isHex = false;
        }
      }
      return isHex;
    } else {
      return false;
    }
  }

  public toString() {
    if(this.strValue === EMPTY_STRING) {
      for (var i = 0; i < this.data.length; i++) {
        var octet = this.data[i];
        var str = octet.toString(16);
        this.strValue += str.length === 2 ? str : "0" + str;
      }
    }
    return this.strValue;
  }

}