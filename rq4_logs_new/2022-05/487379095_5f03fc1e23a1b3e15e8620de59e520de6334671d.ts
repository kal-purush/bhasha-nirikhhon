import { useEffect, useState, useCallback, Dispatch } from 'react';

const breakpointCheck = (maxWidth: number | string, setter: Dispatch<boolean>) => {
  if (window.matchMedia(`(max-width: ${maxWidth}px)`).matches) {
    setter(true);
  } else {
    setter(false);
  }
};

export const useIsMobile = () => {
  const [isMobile, setIsMobile] = useState(false);

  const onResize = useCallback(() => {
    breakpointCheck(599, setIsMobile);
  }, []);

  useEffect(() => {
    onResize();

    window.addEventListener('resize', onResize);

    return () => {
      window.removeEventListener('resize', onResize);
    };
  }, []);

  return isMobile;
};

export const useIsTablet = () => {
  const [isTablet, setIsTablet] = useState(false);

  const onResize = useCallback(() => {
    breakpointCheck(768, setIsTablet);
  }, []);

  useEffect(() => {
    onResize();

    window.addEventListener('resize', onResize);

    return () => {
      window.removeEventListener('resize', onResize);
    };
  }, []);

  return isTablet;
};