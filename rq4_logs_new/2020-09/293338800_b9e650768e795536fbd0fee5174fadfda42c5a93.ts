import { ReactNode } from "react";

export const renderNTimes = (component: ReactNode, n = 10) => {
  return [...Array(n)].map((_, i) => component);
};