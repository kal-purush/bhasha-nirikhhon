import { apiUrl } from '../apiconfigs/apiUrl';
import { UserRequest } from '../user/userRequest';

export const registerInteractor = async (userRequest: UserRequest) => {
  const res = await fetch(apiUrl('/auth/register'), {
    method: 'post',
    body: JSON.stringify(userRequest),
    headers: { 'Content-Type': 'application/json' },
  });

  if (res.ok) {
    console.debug(res);
    console.debug('Register SUCCESS');
    return true;
  }

  console.debug('Register FAILED');
  console.debug(res);
  return false;
};