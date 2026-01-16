import { cache } from 'react';
import { getRestApiQuery } from '@/api/rest/api';
import { Item } from '@/types/item';

const LOCATION_ROOT = '!ZEZxbNWFYYsDgpkhCL:content.udk-berlin.de';

export const getContextTree = cache(async (id: string) => {
  const res = await fetch(getRestApiQuery(`${id}/tree`));
  return res.json();
});

export const getLocationList = cache(async (id: string) => {
  const res = await fetch(getRestApiQuery(`${id}/list/filter/type/item`));
  return res.json();
});

export const getLocationItems = cache(async (id?: string) => {
  id = id ?? LOCATION_ROOT;
  const res = await getLocationList(id);
  return res.map((item: Item) => item.id);
});

export const getLocation = cache(async (id: string): Promise<any> => {
  const item = await getContextTree(id);
  return {
    id: item.id,
    name: item.name,
    thumbnail: item.thumbnail,
    thumbnail_full_size: item.thumbnail_full_size,
    descriptions: item.description,
    languages: Object.keys(item.description),
  };
});