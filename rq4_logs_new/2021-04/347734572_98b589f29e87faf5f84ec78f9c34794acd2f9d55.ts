import { useRef, useEffect } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import type { Location } from 'history';

import { UnauthenticatedError } from '../api';

type RedirectFn = (
  error: unknown,
  fn?: (error: Error | unknown) => void
) => void;

export const useRedirectUnauthenticatedUser = (): RedirectFn => {
  const history = useHistory();
  const initialLocation = useLocation();
  const location = useRef<Location>(initialLocation);
  const redirect = useRef<RedirectFn>((error, fn) => {
    if (error instanceof UnauthenticatedError) {
      const from = `${location.current.pathname}${location.current.search}`;

      history.replace('/login', location.current.state || { from });
    } else {
      if (typeof fn === 'function') {
        fn(error);
      } else {
        throw error;
      }
    }
  });

  useEffect(
    () =>
      history.listen((newLocation) => {
        location.current = newLocation;
      }),
    [history]
  );

  return redirect.current;
};