import {$authHost, $host} from './index';
import {decodeToken} from "react-jwt";

export const authorization = async (formData) => {
    const { data } = await $host.post('api/auth/login', formData);
    return data;
}

export const registration = async (formData) => {
    const { data } = await $host.post('api/auth/registration', formData);
    return data;
}

export const checkAuth = async () => {
    const { data } = await $authHost.get('api/auth/check-auth');
    if (data.token) {
        return decodeToken(data.token);
    }
}