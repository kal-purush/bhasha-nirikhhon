import { useState, useCallback } from "react";
import axios, { AxiosRequestConfig, AxiosResponse } from "axios";

export type Config = AxiosRequestConfig;

const useFetch = () => {
  const [working, setWorking] = useState(false);

  const fetchJSON = useCallback(
    async <T>(req: AxiosRequestConfig): Promise<T | null> => {
      const res = await sendRequest<T>(req);
      if (!res) return null;
      return res.data;
    },
    [],
  );

  const deleteData = useCallback(
    async (req: AxiosRequestConfig): Promise<true | null> => {
      const res = await sendRequest(req);
      if (!res) return null;
      return true;
    },
    [],
  );

  const postRequest = async <T>(req: AxiosRequestConfig): Promise<T | null> => {
    const res = await sendRequest<T>(req);
    if (!res) return null;
    return res.data;
  };

  const sendRequest = async <T>(req: AxiosRequestConfig) => {
    setWorking(true);
    try {
      const res: AxiosResponse<T> = await axios(req);
      setWorking(false);
      return res;
    } catch (error) {
      console.error(error);
      setWorking(false);
      return null;
    }
  };

  return { fetchJSON, postRequest, deleteData, working };
};

export default useFetch;