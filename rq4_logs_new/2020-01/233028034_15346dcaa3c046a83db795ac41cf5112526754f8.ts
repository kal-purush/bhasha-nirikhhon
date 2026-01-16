import { Restaurant } from '../../interfaces/Orders';
import { FetchResult } from '../../interfaces/Commons';
import useFetcher from './useFetcher';

const useOrders = (): FetchResult<Restaurant[]> => {
  return useFetcher<Restaurant[]>(
    () =>
      new Promise<Restaurant[]>(resolve => {
        setTimeout(
          () =>
            resolve([
              {
                key: 0,
                title: 'test',
                merchant: 'merchant 1',
                genres: ['pork', 'chicken'],
                allergics: ['seafood']
              },
              {
                key: 1,
                title: 'test',
                merchant: 'merchant 1',
                genres: ['pork', 'chicken']
              },
              {
                key: 2,
                title: 'test',
                merchant: 'merchant 1',
                genres: ['pork', 'chicken']
              }
            ] as Restaurant[]),
          300
        );
      })
  );
};