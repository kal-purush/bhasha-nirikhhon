import axios from "axios";

export const fetchData = async (endpoint: string): Promise<any> => {
  const response = await axios.get(endpoint);
  return response.data;
};

export const postData = async (endpoint: string, data: any): Promise<any> => {
  const response = await axios.post(endpoint, data);
  return response.data;
};

export const updateData = async (endpoint: string, data: any): Promise<any> => {
  const response = await axios.put(endpoint, data);
  return response.data;
};

export const patchData = async (endpoint: string, data: any): Promise<any> => {
  const response = await axios.patch(endpoint, data);
  return response.data;
};

export const deleteData = async (endpoint: string): Promise<any> => {
  const { data } = await axios.delete(endpoint);
  return data;
};