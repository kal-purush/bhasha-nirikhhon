import {Request} from '@idelic/safety-net';

import {runApi} from '../../runApi';
import {ApiOptions} from '../../types';
import {InputModel} from './types';

export function getModelDuplicates(
  params: InputModel<any, any>,
  apiOptions?: ApiOptions
): Request<number[]> {
  return runApi({
    method: 'POST',
    urlRoot: 'apiUrlRoot',
    route: '/api/models/duplicates/get',
    apiOptions,
    requestOptions: {
      body: params
    }
  });
}