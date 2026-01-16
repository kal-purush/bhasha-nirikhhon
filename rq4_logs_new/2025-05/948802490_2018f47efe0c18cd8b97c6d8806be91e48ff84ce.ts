import { TRegisterFacilitator } from '@/types/facilitatorTypes';
import axios from 'axios';

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:2000/api';

export const putToken = (token: string) => {
  localStorage.setItem('accessToken', token);
};

export const getToken = () => {
  const token = localStorage.getItem('accessToken');
  return token;
};

export const getAllFacilitator = async () => {
  const token = getToken();
  try {
    const response = await axios.get(`${baseURL}/facilitator`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    return response.data;
  } catch (error: any) {
    if (error.response && error.response.data) {
      return error.response.data;
    }
    return { success: false, message: 'Terjadi Kesalahan!', data: null };
  }
};

export const createFacilitator = async (formData: TRegisterFacilitator) => {
  const token = getToken();
  try {
    const response = await axios.post(
      `${baseURL}/facilitator/register`,
      formData,
      {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    );
    return response.data;
  } catch (error: any) {
    if (error.response && error.response.data) {
      return error.response.data;
    }
    return { success: false, message: 'Terjadi Kesalahan!', data: null };
  }
};