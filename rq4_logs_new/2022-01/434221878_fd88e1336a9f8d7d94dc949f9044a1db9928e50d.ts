// password-transformer.ts

import { ConfigService } from '@nestjs/config';
import { hashSync } from 'bcryptjs';
import { ValueTransformer } from 'typeorm';

export class PasswordTransformer implements ValueTransformer {
  constructor(private config: ConfigService) {}

  // eslint-disable-next-line class-methods-use-this
  from(v: string): string {
    return v;
  }

  to(password: string) {
    // ! async transformers are not supported at the moment in typeorm
    // https://github.com/typeorm/typeorm/pull/885#issuecomment-337920537
    return hashSync(
      password,
      +(this.config.get<number>('BCRYPT_SALT_ROUNDS') || 10),
    );
  }
}

// __EOF__