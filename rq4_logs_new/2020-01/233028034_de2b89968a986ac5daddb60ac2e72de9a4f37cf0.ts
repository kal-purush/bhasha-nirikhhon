import { Order } from '../../interfaces/interface.order';

const useMockApi = (arg: string) => {
  let api: (() => any) | null = null;
  switch (arg) {
    case 'order':
      api = (): Order[] => [
        {
          title: 'test',
          merchant: 'merchant 1',
          genres: ['pork', 'chicken'],
          allergics: ['seafood']
        },
        {
          title: 'test',
          merchant: 'merchant 1',
          genres: ['pork', 'chicken']
        },
        {
          title: 'test',
          merchant: 'merchant 1',
          genres: ['pork', 'chicken']
        }
      ];
      break;
    default:
      api = null;
      break;
  }
  return new Promise((resolve, reject) => {
    if (api === null) reject();
    setTimeout(() => resolve(api), 500);
  });
};

export default useMockApi;