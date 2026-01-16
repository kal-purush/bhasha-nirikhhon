import axios from "axios";

const baseURL = "http://localhost:2000/api";

type Tregister = {
  firstName: string;
  lastName: string;
  email: string;
  phoneNumber: string;
  dateOfBirth: string;
  gender: number | null;
  password: string;
  confirmPassword: string;
};

export const registerAccount = async (formData: Tregister) => {
  try {
    const response = await axios.post(`${baseURL}/auth/register`, formData);

    const { data, message } = response.data;

    console.log("Data:", data);
    console.log("Message:", message);
    return response.data;
  } catch (error: any) {
    const err = error as unknown as Error;
    throw err;
  }
};