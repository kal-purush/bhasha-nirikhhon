import BaseAPI from '../../../utils/BaseAPI';
import httpTransport from '../../../utils/HTTPTransport';

export type ProfileData = {
    email: string;
    login: string;
    first_name: string;
    second_name: string;
    display_name: string;
    phone: string;
}

class UserSettingsAPI extends BaseAPI {
  deleteSession() {
    return httpTransport.post('/auth/logout', {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
      },
    });
  }

  update(data: ProfileData) {
    return httpTransport.put('/user/profile', {
      data: JSON.stringify(data),
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
      },
    });
  }

  updateAvatar(data: FormData) {
    return httpTransport.put('/user/profile/avatar', {
      data,
    });
  }

  request() {
    return httpTransport.get('/auth/user', {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
      },
    });
  }
}

export default new UserSettingsAPI();