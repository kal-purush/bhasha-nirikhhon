import { createClient, EntryCollection } from 'contentful';
import { config, transformContentfulError } from './index';
import {DataResponse, HttpResponse} from '@/pages/api/types';

// TODO EntryCollection のジェネリクス引数は一旦anyで
export const getEntries = (): Promise<HttpResponse<EntryCollection<any>>> => {
  return createClient(config)
    .getEntries()
    .then((entries) => {
      const response: DataResponse<EntryCollection<any>> = {
        data: entries,
        err: null,
        status: 200,
      };
      return Promise.resolve(response);
    })
    .catch((err) => {
      return transformContentfulError(err);
    });
};