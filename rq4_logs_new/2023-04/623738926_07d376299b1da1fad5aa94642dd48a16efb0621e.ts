import { axiosInstance } from "./axiosConfig";

export const API = {
  login: <T>(data: T) => {
    return axiosInstance({
      method: "POST",
      url: `/api/auth/login`,
      data,
      headers: {
        "Content-Type": "application/json",
      },
    });
  },
  getUser: (memberId: number) => {
    return axiosInstance({ method: "GET", url: `/api/member/${memberId}` });
  },
};