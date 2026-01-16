import { useState, useEffect } from "react";

const useVisibility = () => {
  const [pageVisible, setPageVisible] = useState(false);

  // Handle Page Visibility Changes
  useEffect(() => {
    function handleVisibilityChange() {
      if (document.hidden) {
        setPageVisible(false);
      } else {
        setPageVisible(true);
      }
    }
    document.addEventListener(
      "visibilitychange",
      handleVisibilityChange,
      false
    );
    window.addEventListener("focus", () => setPageVisible(true), false);
    window.addEventListener("blur", () => setPageVisible(false), false);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", () => setPageVisible(true));
      window.removeEventListener("blur", () => setPageVisible(false));
    };
  }, []);

  return { pageVisible };
};

export default useVisibility;