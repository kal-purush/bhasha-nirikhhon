import axios from "axios";
import { TRegister, TLogin } from "@/types/authTypes";

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:2000/api";

export const registerAccount = async (formData: TRegister) => {
  try {
    const response = await axios.post(`${baseURL}/auth/register`, formData);
    return response.data;
  } catch (error: any) {
    if (error.response && error.response.data) {
      return error.response.data;
    }
    return { success: false, message: "Something went wrong", data: null };
  }
};

export const loginAccount = async (formData: TLogin) => {
  try {
    const response = await axios.post(`${baseURL}/auth/login`, formData);
    return response.data;
  } catch (error: any) {
    if (error.response && error.response.data) {
      return error.response.data;
    }
    return { success: false, message: "Something went wrong", data: null };
  }
};