
import {HashType} from './enums/hash_type';
import {SHA256} from './../node_modules/fast-sha256/sha256';



export class HMAC {

  static createHMAC(key: Uint8Array, data: Uint8Array, hash: HashType): Uint8Array {

    if(hash === HashType.SHA256) {
      return SHA256.HMAC(key, data);
    } else {
      throw 'Unimplemented';
    }

  }

}