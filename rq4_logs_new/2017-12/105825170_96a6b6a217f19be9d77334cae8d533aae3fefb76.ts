import { Storage } from '@ionic/storage';
import { Headers, RequestOptions } from '@angular/http';

export class AuthenticationService {
  constructor(private storage: Storage) {}

  public async buildAuthenticationRequest(): Promise<RequestOptions> {
    const userToken = await this.storage.get('userToken');
    const headers = new Headers();
    headers.append('Authorization', userToken);
    return new RequestOptions({ headers: headers });
  }

  public async getUserToken(): Promise<string> {
    return await this.storage.get('userToken');
  }

  public storeUserToken(data: any): void {
    this.storage.set('userToken', data.id);
    this.storage.set('userId', data.userId);
  }

  public clearUserToken(): void {
    this.storage.set('userToken', null);
    this.storage.set('userId', null);
  }
}