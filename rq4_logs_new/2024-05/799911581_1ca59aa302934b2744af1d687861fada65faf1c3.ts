import { useCallback, useState } from 'react';

import { useIsomorphicLayoutEffect } from './use-isomorphic-layout-effect';

const MEDIA_QUERY = {
  XS: 'only screen and (max-width: 320px)' as const,
  SM: 'only screen and (max-width: 639px)' as const,
  MD: 'only screen and (min-width: 640px) and (max-width: 1279px)' as const,
  LG: 'only screen and (min-width: 1280px)' as const,
};
type MediaQueryValues = (typeof MEDIA_QUERY)[keyof typeof MEDIA_QUERY];

/**
 * Match query using window object
 * @param query media query e.g. "(min-width: 600px)"
 * @returns `true` if query match, otherwise false.
 */
const getMatches = (query: MediaQueryValues): boolean => {
  if (typeof window !== 'undefined') {
    return window.matchMedia(query).matches;
  }

  return false;
};

/**
 *
 * @param query {@link MEDIA_QUERY} values
 * @param ssrDefault default value returned during ssr
 * @returns `true` if query match otherwise `false`
 */
const useMediaQuery = (
  query: MediaQueryValues,
  ssrDefault?: boolean,
): boolean => {
  const [matches, setMatches] = useState<boolean>(!!ssrDefault);

  const handleChange = useCallback(
    () => setMatches(getMatches(query)),
    [query],
  );

  useIsomorphicLayoutEffect(() => {
    handleChange();

    const matchMedia = window.matchMedia(query);
    matchMedia.addEventListener('change', handleChange);

    return () => matchMedia.removeEventListener('change', handleChange);
  }, [handleChange, query]);

  if (typeof window === 'undefined') return Boolean(ssrDefault);

  return matches;
};

export const useIsXs = (ssrDefault?: boolean) =>
  useMediaQuery(MEDIA_QUERY.XS, ssrDefault);

export const useIsMobile = (ssrDefault?: boolean) =>
  useMediaQuery(MEDIA_QUERY.SM, ssrDefault);

export const useIsTablet = (ssrDefault?: boolean) =>
  useMediaQuery(MEDIA_QUERY.MD, ssrDefault);

export const useIsDesktop = (ssrDefault?: boolean) =>
  useMediaQuery(MEDIA_QUERY.LG, ssrDefault);