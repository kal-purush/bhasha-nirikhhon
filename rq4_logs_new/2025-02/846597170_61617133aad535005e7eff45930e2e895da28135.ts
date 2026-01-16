//auth/signin

import axios from "./axios";
const SignUrl = "/api/auth/signin";
interface SigninPayLoad{
    email: string;
    password: string
}

export const signinUser = async (userData: SigninPayLoad)=>{
    try{
        const response = await axios.post(SignUrl, userData)
        return response.data

    }catch(error: unknown){
        throw error
    }
}