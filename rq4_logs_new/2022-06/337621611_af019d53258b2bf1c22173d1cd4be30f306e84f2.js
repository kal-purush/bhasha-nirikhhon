import { useContext, createContext } from "react";

export const DurationContext = createContext({
  duration: null,
  setDuration: () => {},
});

export default function useDuration() {
  return useContext(DurationContext);
}