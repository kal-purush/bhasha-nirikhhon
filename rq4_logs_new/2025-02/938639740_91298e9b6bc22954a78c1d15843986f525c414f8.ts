import { api } from "../utils/api";

export const getDashboardStats = async () => {
  const response = await api.get("/stat");
  return response.data.data;
};