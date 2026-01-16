import { useMemo } from 'react';

interface MobilePlatformInfo {
  isAndroid: boolean;
  isIOS: boolean;
  isMobileDevice: boolean;
}

/**
 * Determine the mobile platform information.
 */
export const useMobilePlatform = (): MobilePlatformInfo => {
  const platformInfo = useMemo(() => {
    const userAgent = navigator.userAgent;
    const isAndroid = /Android/i.test(userAgent);
    const isIOS = /iPad|iPhone|iPod/.test(userAgent);

    return { isAndroid, isIOS, isMobileDevice: isAndroid || isIOS };
  }, []);

  return platformInfo;
};