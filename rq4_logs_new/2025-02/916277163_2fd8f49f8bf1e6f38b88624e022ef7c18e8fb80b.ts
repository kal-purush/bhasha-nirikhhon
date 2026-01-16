import { Http } from '@repo/services/http/http';

import type { ISignInParams, ISignUpParams, IUser } from './interface';

export class Auth extends Http {
  constructor(baseUrl: string, headers: Record<string, string>) {
    super(baseUrl, { headers });
  }

  public async signUp(params: ISignUpParams): Promise<{ message: string }> {
    return this.post('auth/signUp', { body: params });
  }

  public async signIn(params: ISignInParams): Promise<{ token: string }> {
    return this.post('auth/signIn', { body: params });
  }

  public async getOne(id: string): Promise<IUser> {
    return this.get(`auth/${id}`);
  }

  public async me(): Promise<IUser> {
    return this.get('auth/me');
  }
}