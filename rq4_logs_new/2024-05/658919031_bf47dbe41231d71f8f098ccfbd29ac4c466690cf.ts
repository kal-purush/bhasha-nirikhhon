import { useEffect, useState } from 'react';

import type { IPosition } from 'store/features/weather-widget-data/types';

import type { IUsePosition } from '../types';

export const usePosition: IUsePosition = () => {
  const [position, setPosition] = useState<IPosition>({} as IPosition);
  const [navigatorError, setNavigatorError] = useState(false);
  const [navigatorPrompt, setNavigatorPrompt] = useState(false);
  const cityFromLS = localStorage.getItem('city');

  useEffect(() => {
    const success = (pos: GeolocationPosition) => {
      const latitude = pos.coords.latitude;
      const longitude = pos.coords.longitude;

      !cityFromLS &&
        setPosition({
          lat: latitude,
          lon: longitude,
        });
      setNavigatorPrompt(false);
    };

    const error = (err: GeolocationPositionError) => {
      if (err.PERMISSION_DENIED) {
        setNavigatorError(true);
        setNavigatorPrompt(false);
      }
      console.error(err);
    };

    navigator.geolocation.getCurrentPosition(success, error);

    navigator.permissions.query({ name: 'geolocation' }).then((permissionStatus) => {
      if (permissionStatus.state === 'prompt') {
        setNavigatorPrompt(true);
      }
    });
  }, []);

  const isPosition = !!(position.lat && position.lon);

  return { position, navigatorError, navigatorPrompt, isPosition, cityFromLS };
};