import axiosInstance from "./axiosInstance";
import { Endpoints } from "./endpoints";

export const login = async (email: string, password: string) => {
  try {
    const response = await axiosInstance.post(Endpoints.login, {
      email,
      password,
    });
    return response.data;
  } catch (error: any) {
    return error.response.data;
  }
};

export const getProjectTickets = async ({ queryKey }: { queryKey: any }) => {
  const [_key, id, page] = queryKey;
  const res = await axiosInstance.get(
    `${Endpoints.getProjectTickets}/${id}?page=${page}`
  );
  return res.data;
};

export const createTicket = async (payload: any) => {
  await axiosInstance.post(`${Endpoints.createTicket}`, payload);
};