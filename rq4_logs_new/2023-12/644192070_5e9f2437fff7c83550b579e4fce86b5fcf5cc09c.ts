import { config } from 'config';
import { getRequestOptions } from 'helpers/getRequestOptions';
import { IpGeolocationDataI } from 'types/types';

export async function getIpGeolocationData(): Promise<IpGeolocationDataI> {
  // &include=security - could be included only for paid plans
  const data = await fetch(
    `https://api.ipgeolocation.io/ipgeo?apiKey=${config.ipGeolocationApiKey}&fields=geo`,
    getRequestOptions()
  );
  if (!data.ok) {
    console.error({ data });
    throw new Error(data.statusText);
  }
  return data.json();
}