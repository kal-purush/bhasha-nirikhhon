import axios from 'axios';
import {API_BASE_URL} from "./apiService.ts";
const LOGIN_API_ENDPOINT = `${API_BASE_URL}/login`;
const REGISTER_API_ENDPOINT = `${API_BASE_URL}/register`;

export const loginUserAPI = async (payload: {
    email?: string;
    phone_number?: string;
    password?: string;
    verification_code?: string;
    step?: "send_code" | "verify_code" | "skip_verification";
    login_type?: "email" | "phone_number";
    password_login?: boolean;
}) => {
    console.log('Sending login request with payload:', payload);
    const response = await axios.post(LOGIN_API_ENDPOINT, payload);
    console.log('Received login response:', response.data);
    return response.data;
};

export const registerUserAPI = async (userData: {
    name_surname: string;
    email?: string;
    phone_number?: string;
    password: string;
}) => {
    try {
        console.log('Sending registration request with userData:', userData);
        const response = await axios.post(REGISTER_API_ENDPOINT, userData);
        console.log('Received registration response:', response.data);
        return response.data;
    } catch (error) {
        console.error('Error during user registration:', error);
        throw error;
    }
};
