import { useEffect, useRef } from "react";

export const useClickOutside = (callback: () => void) => {
  const ref = useRef(null);

  useEffect(() => {
    const onClickCallback = (e: MouseEvent) => {
      // If target is child of ref do nothing
      if (ref.current && ref.current.contains(e.target)) {
        return;
      }
      callback();
    };
    window.addEventListener("click", onClickCallback, true);

    return () => {
      window.removeEventListener("click", onClickCallback, true);
    };
  }, [ref]);

  return { ref };
};