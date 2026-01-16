// api/userAPI.ts
import axios from 'axios';

const API_BASE_URL = 'https://freshdealapi-fkfaajfaffh4c0ex.uksouth-01.azurewebsites.net';

const LOGIN_API_ENDPOINT = `${API_BASE_URL}/v1/login`;
const REGISTER_API_ENDPOINT = `${API_BASE_URL}/v1/register`;
const GET_USER_DATA_API_ENDPOINT = `${API_BASE_URL}/v1/user/data`;

// Flexible Login API Call
export const loginUserAPI = async (payload: {
    email?: string;
    phone_number?: string;
    password?: string;
    verification_code?: string;
    step?: "send_code" | "verify_code" | "skip_verification";
    login_type?: "email" | "phone_number";
    password_login?: boolean;
}) => {
    const response = await axios.post(LOGIN_API_ENDPOINT, payload);
    return response.data;
};

// User Registration API Call
export const registerUserAPI = async (userData: {
    name_surname: string;
    email?: string;
    phone_number?: string;
    password: string;
}) => {
    try {
        const response = await axios.post(REGISTER_API_ENDPOINT, userData); // Adjust the endpoint
        console.log('Request URL:', axios.getUri({method: 'POST', url: REGISTER_API_ENDPOINT})); // Logs full URL
        return response.data;
    } catch (error) {
        console.error('API Request Error:', {
            message: error.message,
            config: error.config,
            stack: error.stack, // Log error stack trace
        });
        throw error; // Re-throw error for thunk to handle
    }
};




export const getUserDataAPI = async (token: string) => {
    const response = await axios.get(GET_USER_DATA_API_ENDPOINT, {
        headers: {Authorization: `Bearer ${token}`}
    });
    return response.data;
}