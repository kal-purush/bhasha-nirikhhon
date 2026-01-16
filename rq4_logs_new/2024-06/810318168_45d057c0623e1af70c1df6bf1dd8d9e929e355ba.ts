import { useState, useEffect, useCallback } from "react";

export const useFetch = (url: string) => {
  const [data, setData] = useState(null);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState("");

  const fetchData = useCallback(async () => {
    setIsPending(true);
    try {
      const response = await fetch(url);
      if (!response.ok) throw new Error(response.statusText);
      const json = await response.json();
      setIsPending(false);
      setData(json);
      setError("");
    } catch (error) {
      setError("Could not fetch data");
      setIsPending(false);
    }
  }, [url]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const refetch = () => {
    fetchData();
  };

  return { data, isPending, error, refetch };
};