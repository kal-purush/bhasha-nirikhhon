import { LocationResult, Location } from '@communecar/types';
import axios from 'axios';

import { axiosClient } from '../client';
import { isArray, isUndefined } from 'lodash';

const UNKNOWN_LOCATION = 'Unknown location 😵‍💫';

const geocode = async (coords: {
  lat: number;
  lon: number;
}): Promise<string> => {
  try {
    const response = await axiosClient.get<LocationResult[]>(
      '/api/v1/external/reverse-geocode',
      {
        params: { lat: coords.lat, lon: coords.lon },
      },
    );

    if (response.data) {
      const data = response.data;
      if (!isArray(data)) {
        const newData = data as LocationResult;
        return newData.displayName || newData.name;
      } else if (isArray(data) && data.length > 0 && !isUndefined(data[0])) {
        return response.data[0].name || response.data[0].displayName;
      } else {
        console.error('Geocoding error');
        return UNKNOWN_LOCATION;
      }
    }
    throw new Error('data missing');
  } catch (error) {
    console.error('Geocoding error:', error);
    if (
      axios.isAxiosError(error) &&
      error.response &&
      error.response.status === 404
    ) {
      console.log(error);
      return UNKNOWN_LOCATION;
    }
    console.log(error);
    return 'An extremely unknown location 😵‍💫😵‍💫';
  }
};

const locationExtraction = async (node: {
  long?: number;
  lat?: number;
  baseLocationName?: string;
}): Promise<Location | undefined> => {
  const name = node.baseLocationName
    ? node.baseLocationName
    : !isUndefined(node.lat) && !isUndefined(node.long)
      ? await geocode({
          lat: node.lat,
          lon: node.long,
        })
      : undefined;

  const coordinates: Location | undefined =
    node.lat && node.long
      ? {
          lat: node.lat,
          lon: node.long,
          name,
        }
      : undefined;

  return coordinates;
};

export { geocode, locationExtraction };