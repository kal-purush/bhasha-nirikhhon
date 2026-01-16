import { Base } from './Base'
import { AUTHENTICATION_SERVICE_ORIGIN } from './origins'

export interface IAuth {
  name: string;
  picture: string; 
  auth_time: number;
  email: string;
  email_verified: boolean;
  uid: string;
}

export class Authentication extends Base {
  private static get origin() {
    return process.env.REMAP_AUTHENTICATION_SERVICE_ORIGIN || AUTHENTICATION_SERVICE_ORIGIN
  }

  public static async authenticate(idToken: string): Promise<IAuth> {
    return await this.post(`${this.origin}/`, {
      body: JSON.stringify({ id_token: idToken }),
    })
  }
}