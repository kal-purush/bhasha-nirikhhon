import { useEffect, useState } from "react";
import { METHOD } from "../utils/constants";
import { callApi } from "./callApi";

export const UseApiRequest = (
  method: METHOD,
  url: string,
  data?: { [key: string]: any } | undefined
): any => {
  const [response, setResponse] = useState<{ [key: string]: any }>({});
  const [error, setError] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    const asyncFn = async () => {
      try {
        const request = await callApi(method, url, data);
        setResponse(request);
      } catch (error) {
        setError(true);
      } finally {
        setLoading(false);
      }
    };
    asyncFn();
  }, [url]);

  return { response, error, loading };
};