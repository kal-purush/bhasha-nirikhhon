import axios from "axios";
import { configOptions } from "../config/config";
const { apiAddress } = configOptions;

export const projectsIndex = async () => {
  const res = await axios.get(`${apiAddress}/api/projects`);
  return res.data;
};