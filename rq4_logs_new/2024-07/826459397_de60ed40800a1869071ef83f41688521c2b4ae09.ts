import axios from "axios";
import { deleteCookie, getCookie, setCookie } from "cookies-next";

const BASE_URL = "http://localhost:8000/api";

export const api = axios.create({
  baseURL: BASE_URL,
});

api.interceptors.request.use((config) => {
  if (config.url !== "/auth/token") {
    const accessToken = getCookie("accessToken");
    config.headers.Authorization = "Bearer" + accessToken;
  }
  return config;
});

api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    const config = error.config;
    if (error.response.status === 401 && !config.sent) {
      config.sent = true;
      if (config.url !== "/auth/token" && config.url !== "/auth/login") {
        const refreshToken = getCookie("refreshToken");
        return api.post("/auth/token", { refreshToken }).then((res) => {
          const accessToken = res.data.token.accessToken;
          setCookie("accessToken", accessToken);
          config.headers.Authorization = "Bearer " + accessToken;
          return api(config);
        });
      } else if (config.url === "/auth/token") {
        deleteCookie("accessToken");
        deleteCookie("refreshToken");
        location.href = "/login";
      }
    } else {
      return error.response;
    }
  }
);