import { cache } from 'react';
import { getRestApiQuery } from '@/api/rest/api';
import { LOCATION_ROOT } from '../constants';
import { Context, GraphQlItem } from '@/types/graphql';

export const getEventList = cache(
  async (id: string): Promise<GraphQlItem[]> => {
    const res = await fetch(
      getRestApiQuery(`${id}/list/filter/allocation/temporal`),
    );
    return res.json();
  },
);

export const getEventLocations = cache(async (): Promise<Context[]> => {
  const res = await fetch(getRestApiQuery(`${LOCATION_ROOT}/tree`));
  return res.json().then((r) => Object.values(r.children));
});