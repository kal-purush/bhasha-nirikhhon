import { AxiosResponse } from 'axios';

import { IUserRepository, TCurrentUserResponse } from '@src/repository/UserRepository/types';

export interface IUserService {
  getCurrentUser(): Promise<AxiosResponse<TCurrentUserResponse>>;
}

export class UserService {
  constructor(private _repo: IUserRepository) {}

  getCurrentUser() {
    return this._repo.getCurrentUser();
  }
}