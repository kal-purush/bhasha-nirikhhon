import { PATHS_CORE } from "common/constants/paths";
import { useHistory } from "react-router-dom";
import { useCallback, useEffect } from "react";
import { LOCALSTORAGE_AUTH_TOKENS } from "common/constants/auth";

const useTokenListener = () => {
  const history = useHistory();

  const handleStorageChange = useCallback(
    (e: StorageEvent) => {
      if (e.key === LOCALSTORAGE_AUTH_TOKENS && e.newValue === null) {
        history.push(PATHS_CORE.LOGOUT);
      }
    },
    [history]
  );

  useEffect(() => {
    window.addEventListener("storage", handleStorageChange);

    return () => {
      window.removeEventListener("storage", handleStorageChange);
    };
  }, [handleStorageChange]);
};

export default useTokenListener;