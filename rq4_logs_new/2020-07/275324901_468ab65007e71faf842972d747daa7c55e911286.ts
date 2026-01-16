import * as crypto from 'crypto';

export const passwordWrap = (pwd: string, salt: string) => {
    return crypto
      .createHmac('sha256', salt)
      .update(pwd)
      .digest('hex')
}