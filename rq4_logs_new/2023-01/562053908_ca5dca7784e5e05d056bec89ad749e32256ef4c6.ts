/* Local dependencies */
import { getRequest, postRequest } from './axois-api';

export async function getSubscriptions(token: string) {
  const response = await getRequest(token, 'v1/user/subscriptions');

  return response;
}

// TODO(murat): put proper type
export async function subscribe(token: string, data: any) {
  const response = await postRequest(token, '/v1/user/subscriptions/', data);

  return response;
}

const subscriptionsService = {
  getSubscriptions,
  subscribe,
};

export default subscriptionsService;