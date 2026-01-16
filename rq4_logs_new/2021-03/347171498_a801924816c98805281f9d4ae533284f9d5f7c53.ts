import * as crypto from 'crypto';
import { isNil } from 'lodash';
import { Service } from 'typedi';

@Service()
export class CryptoService {
  generateHash(value: string): string {
    return crypto.createHash('sha256').update(value).digest('base64');
  }

  generateHashWithSalt(value: string, salt: string): string {
    if (isNil(salt) || salt.length === 0) {
      throw Error('Invalid salt');
    }

    const passwordWithSalt = value + salt;
    return this.generateHash(passwordWithSalt);
  }

  generateRandomPassword(): string {
    return crypto.randomBytes(20).toString('hex');
  }
}