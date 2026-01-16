import { LatLngLiteral } from "leaflet";

export interface Location {
  longitude: number;
  latitude: number;
  timezone: string;
}

export interface SurvivorLocationWrite {
  survivor_id: number;
  position: Location;
}

export function convertLocation(position: LatLngLiteral): Location {
  return { latitude: position.lat, longitude: position.lng, timezone: 'UTC' };
}

export function convertPosition(location: Location): LatLngLiteral {
  return { lat: location.latitude, lng: location.longitude }
}

export function buildLocationString(location?: Location) {
  if (location) {
    const latDirection = location.latitude > 0 ? 'N' : 'S';
    const lngDirection = location.longitude > 0 ? 'E' : 'W';

    const lat = Math.abs(location.latitude).toFixed(4);
    const lng = Math.abs(location.longitude).toFixed(4);

    return `${latDirection}: ${lat} ${lngDirection}: ${lng} (${location.timezone})`;
  }

  return 'Unknown Location';
}