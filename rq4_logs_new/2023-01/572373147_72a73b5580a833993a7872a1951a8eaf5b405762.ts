import { apiUrl } from '../../apiconfigs/apiUrl';
import parseUrlResponse from '../parseUrlResponse';
import { Url } from '../Url';
import { UrlResponse } from '../UrlResponse';

export const getLinkList = async (
  containerId: number,
  token: string
): Promise<Url[]> => {
  const headers = new Headers();
  headers.set('Content-Type', 'application/json');
  headers.set('Authorization', token);

  const res = await fetch(apiUrl('/urls'), {
    method: 'get',
    headers: headers,
  });

  if (res.ok) {
    const urlList = (await res.json()) as UrlResponse[];

    console.log(urlList);
    return urlList.map(parseUrlResponse);
  } else {
    console.error(res);
    return [];
  }
};