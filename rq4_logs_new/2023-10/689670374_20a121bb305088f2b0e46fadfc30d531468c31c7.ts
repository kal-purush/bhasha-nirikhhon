import { useAppDispatch } from "@/redux/hooks";
import { toggleTheme } from "@/redux/slice/themeSlice";
import { fetchData } from "@/redux/slice/dataSlice";

import { data as mockData } from "@/mock/data";

export const useInitialize = () => {
  const dispatch = useAppDispatch();

  const setInitialColorMode = () => {
    const getInitialColorMode = () => {
      const storedPreferenceMode = localStorage.getItem("theme");
      const hasStoredPreference = typeof storedPreferenceMode === "string";
      if (hasStoredPreference) {
        return storedPreferenceMode;
      }

      const preference = matchMedia("(prefers-color-scheme): dark");
      const hasMediaQueryPreference = typeof preference.matches === "boolean";
      if (hasMediaQueryPreference) {
        return preference.matches ? "dark" : "light";
      }

      return "light";
    };
    const currentColorMode = getInitialColorMode();
    dispatch(toggleTheme(currentColorMode));
  };
  setInitialColorMode();
};

export default useInitialize;