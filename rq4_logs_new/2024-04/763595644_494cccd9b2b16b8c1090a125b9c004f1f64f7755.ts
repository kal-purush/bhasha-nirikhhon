import axios, { type AxiosError } from 'axios';
import type { QueryFunctionContext } from '@tanstack/react-query';

import type { HierarchyData, SearchHierarchyParams } from './hierarchy.types';

export async function getHierarchyData({
  queryKey: [params],
}: QueryFunctionContext<readonly [SearchHierarchyParams]>): Promise<
  HierarchyData<string>[]
> {
  try {
    const { data } = await axios.post<HierarchyData<string>[]>(
      '/api/hierarchy/search',
      params
    );

    return data;
  } catch (e) {
    const { code, response } = e as AxiosError;

    switch (process.env.NODE_ENV) {
      case 'development': {
        if (code === 'ECONNREFUSED' || response?.status === 404) {
          return [
            ...Array.from({ length: 2 }).map<HierarchyData<string>>(
              (_el, i) => ({
                _id: `${params.superior || ''}_${i + 1}`,
                category: params.category,
                title: `Group ${params.superior || ''}_${i + 1}`,
                description: `Description for Group ${params.superior || ''}_${
                  i + 1
                }, ${
                  i % 2 === 0
                    ? ''
                    : `superior: ${params.superior}, category: ${params.category}`
                }`,
                type: 'group',
              })
            ),
            {
              _id: `${params.superior || ''}_3`,
              category: params.category,
              title: `${params.category} 3`,
              description: `Description for ${params.category} 3, superior: ${params.superior}, category: ${params.category}`,
              type: 'item',
            },
          ];
        }
      }
    }

    return [];
  }
}