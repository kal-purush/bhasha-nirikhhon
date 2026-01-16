import AxiosUtil from "./AxiosUtil";

// Follow https://axios-http.com/docs/req_config for more info
export const learnCorridorService = AxiosUtil.create(
  {
    baseURL: "http://localhost:8089",
    timeout: 500000,
  },
  { addRequestInterceptor: true }
);

export const userApiClient = AxiosUtil.create({
  baseURL: "http://localhost:8089",
  timeout: 500000,
  headers: { "X-Custom-Header": "foobar" },
});

export const authApiClient = AxiosUtil.create({
  baseURL: "http://localhost:8089/auth",
  timeout: 5000000,
  headers: { "X-Custom-Header": "foobar" },
});