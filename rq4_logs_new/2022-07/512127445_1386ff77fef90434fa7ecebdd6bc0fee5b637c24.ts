import Api from './index';

// tempapi.proj.me/api/jhmPCrWjq

const testApis = new Api('https://tempapi.proj.me');

interface ITemp {
  msg: string;
}

export const firstReq = testApis.sendRequest<ITemp>(
  testApis.createUrl({ url: '/api', args: ['jhmPCrWjq'] }),
  {
    method: 'get',
  },
);

export const TempService = async (): Promise<ITemp> => {
  try {
    const response = await firstReq;
    if (response && response.success) {
      const { data } = response;
      if (data) {
        return data;
      }
    }
    throw new Error();
  } catch (err) {
    throw new Error(err);
  }
};