import { InjectionToken } from '@angular/core';
import { Admin } from './admin';
import { Customer } from './customer';
import { NoopUser } from './noop-user';
import { User } from './user';
import { Vendor } from './vendor';
import { AuthState } from '../+state/auth.state';

export const USER_TYPE_TOKEN = new InjectionToken<string>('USER_TYPE_TOKEN');

export const userFactory = (type = 'customer', state?: AuthState): User => {
  const userTypes = {
    admin: Admin,
    customer: Customer,
    vendor: Vendor
  };
  if (!type) return new NoopUser(undefined);
  return userTypes[type] ? new userTypes[type](state) : new NoopUser(undefined);
}