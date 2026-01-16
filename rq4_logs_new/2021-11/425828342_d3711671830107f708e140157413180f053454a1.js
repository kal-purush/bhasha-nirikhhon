import { useState, useEffect } from "react";

export const useCountDown = (initialValue) => {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    const interval = setInterval(() => {
      setValue((value) => value - 1);
    }, 1000);

    return () => {
      clearInterval(interval);
    };
  }, []);

  return [value];
};