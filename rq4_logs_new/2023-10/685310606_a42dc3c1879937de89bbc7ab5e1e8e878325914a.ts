import axios from 'axios';

import { IUser, IUserLogin } from '@/interfaces/IUser';

import { PATH_LOGIN, URL_BASE } from '../constants';

const postLogin = async (request: IUserLogin) => {
  const loginUrl = `${URL_BASE}/${PATH_LOGIN}`;
  try {
    const { data, status } = await axios.post<IUser>(loginUrl, request);
    return { userData: data, status };
  } catch (error) {
    // } catch ({ response: { data, status } }) {
    throw new Error('error msg');
    // return { error };
    // return { data: data.message, status };
  }
};

export default postLogin;