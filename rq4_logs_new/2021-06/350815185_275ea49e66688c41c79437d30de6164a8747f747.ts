import { UsersUser } from './types';

import { Api } from '~modules/Api';
import { Store } from '~modules/Store';

export const Users = {
  async getUsers() {
    const users = Store.get('users');
    if (!users) {
      const data = await Api.getData<UsersUser[]>('/users');
      Store.set('users', data);
    }
    return Store.get('users');
  },
  async getUser(id: number): Promise<UsersUser> {
    const users = await this.getUsers();
    return users.find((user: { id: string | number; }) => Number(user.id) === id);
  },
};