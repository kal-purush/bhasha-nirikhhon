import axios from 'axios';

/*
This module implements access layer for all public apis.
For developer convenience, different sub-apis can be configured on different hosts.
*/

interface Interface {
  url: string;
  params?: object;
}

export const temperatureApiGet = async ({
  url,
  params
}: Interface): Promise<void> => axios.get(url, { params }).then(r => r.data);