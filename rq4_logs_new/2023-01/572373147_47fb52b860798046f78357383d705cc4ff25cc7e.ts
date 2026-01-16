import { apiUrl } from '../../apiconfigs/apiUrl';
import { storeInteractor } from '../../localStorage/storeInteractor';

export const deleteContainer = async (containerId: number) => {
  const headers = new Headers();
  headers.set('Content-Type', 'application/json');
  headers.set('Authorization', storeInteractor.getToken());

  const res = await fetch(apiUrl(`/containers?cid=${containerId}`), {
    method: 'DELETE',
    headers: headers,
  });

  if (res.ok) {
    return true;
  } else {
    console.log(res);
    return false;
  }
};