import { Service } from 'typedi';

import { BaseService } from './BaseService';

@Service()
export class UserService extends BaseService {
  debugger: any;
  public submitRegistration = async (values: any) => {
    await this.fetchFunc('POST', 'api/users', values);
  };

  public submitLogin = async (values: any): Promise<any> => {
    return await this.fetchFunc('POST', 'api/login', values);
  };

  public saveProfileChanges = async (values: any) => {
    await this.fetchFunc('POST', 'api/users', values);
  };
}