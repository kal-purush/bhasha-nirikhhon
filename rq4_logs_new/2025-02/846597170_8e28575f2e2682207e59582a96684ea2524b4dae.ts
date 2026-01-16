import axios from "./axios"

const RegisterUrl = "/auth/register"
interface RegisterPayload {
    firstName: string;
    lastName: string;
    email: string;
    password: string;
    agreeToTerms: boolean;
  }

export const registerUser = async (userData:RegisterPayload)=>{
    try {
        const response = await axios.post(RegisterUrl,userData)
        return response.data
    } catch (error: unknown) {
        
        console.error('Registration failed:', error.response?.data || error.message);
        throw error;
    }
}