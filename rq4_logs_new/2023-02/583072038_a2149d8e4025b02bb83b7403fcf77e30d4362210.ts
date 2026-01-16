import HTTP from '../utils/http';
import BaseAPI from './base-api';

const authAPIBase = new HTTP();

export default class AuthAPI extends BaseAPI {
    signin() {
        return authAPIBase.post('https://ya-praktikum.tech/api/v2/auth/signin/', {
            data: {
                login: 'chausme11',
                password: 'passchausme11',
            },
            headers: {
                'Content-Type': 'application/json',
            },
        });
    }

    request() {
        return authAPIBase.get('https://ya-praktikum.tech/api/v2/auth/user/', {
            withCredentials: true,
        });
    }

    logout() {
        return authAPIBase.post('https://ya-praktikum.tech/api/v2/auth/logout/');
    }
}