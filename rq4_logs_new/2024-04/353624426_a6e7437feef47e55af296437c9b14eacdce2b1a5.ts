import axios from "axios";

export const GH_URL = "https://api.github.com";

export const getUserData = async (userLogin: string) => {
  return axios.get(`${GH_URL}/users/${userLogin}`).then((res) => res.data);
};

export const githubClient = () => {
  return axios.create({ baseURL: GH_URL });
};