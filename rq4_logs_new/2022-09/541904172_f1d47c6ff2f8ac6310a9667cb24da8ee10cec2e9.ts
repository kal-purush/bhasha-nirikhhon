import {
    UserCredentialsInterface,
    UserToInsert,
} from "../../interfaces/user.interfaces";
import axios from "../axiosConfig";

export const signin = async (userCredentials: UserCredentialsInterface) => {
    try {
        const response = await axios.post("/users/signin", userCredentials);

        return response.data;
    } catch (error: any) {
        throw error.response.data;
    }
};

export const signup = async (userData: UserToInsert) => {
    try {
        const response = await axios.post("/users/signup", userData);

        return response.data;
    } catch (error: any) {
        throw error.response.data;
    }
};