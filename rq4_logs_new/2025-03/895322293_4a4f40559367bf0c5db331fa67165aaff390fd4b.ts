import { RootState } from "@react-three/fiber";
import { useEffect, useState } from "react";
import { View } from "../types";

export default function useGetThreeExports(view: View = View.Shared) {
  const exists = window.getThree(view);
  const [threeExports, setThreeExports] = useState<RootState | undefined>(
    exists,
  );

  useEffect(() => {
    // 없으면 있을 때까지 기다린다
    const int = setInterval(() => {
      const found = window.getThree(view);
      if (found) {
        setThreeExports(found);
        clearInterval(int);
      }
    }, 50);

    return () => {
      clearInterval(int);
    };
  }, []);

  return threeExports;
};

export const useSetThreeExports = (view: View = View.Shared) => {
  return (state: RootState) => window.setThree(view, state);
};