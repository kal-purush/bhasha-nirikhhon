import { getFunctions, httpsCallable, connectFunctionsEmulator } from 'firebase/functions';
import { ICreateUserAccountRequestData } from '../types/kyc';

import { ENVIRONMENT_DEV } from '../constants/env';

const isDev = process.env.REACT_APP_ENV === ENVIRONMENT_DEV;

export const createUserKYCDetails = (data: ICreateUserAccountRequestData) => {
  const functions = getFunctions();
  // isDev && connectFunctionsEmulator(functions, 'localhost', 5001);
  const createUserAccount = httpsCallable(functions, 'createUserAccount');
  return createUserAccount(data) as unknown as Promise<ICreateUserAccountRequestData>;
};