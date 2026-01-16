import { useState, useCallback, useEffect } from "react";

/**
 * @description useMediaQuery requires width and return whether screen is smaller than
 * the given param
 * @param width max width of the screen
 * @returns whether screen size is less than the given param
 */
const useMediaQuery = (width: any) => {
  const [targetReached, setTargetReached] = useState(false);

  const updateTarget = useCallback((e: { matches: any }) => {
    if (e.matches) {
      setTargetReached(true);
    } else {
      setTargetReached(false);
    }
  }, []);

  useEffect(() => {
    const media = window.matchMedia(`(max-width: ${width}px)`);
    media.addListener(updateTarget);

    if (media.matches) {
      setTargetReached(true);
    }

    return () => media.removeListener(updateTarget);
  }, []);

  return targetReached;
};

export default useMediaQuery;