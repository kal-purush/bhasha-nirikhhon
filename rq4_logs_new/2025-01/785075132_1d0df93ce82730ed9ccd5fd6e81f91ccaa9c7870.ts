import type { ShippingLabel } from '@hike/types';
import { QueryKey, UseQueryOptions, useQuery } from '@tanstack/react-query';
import { findShippingLabels } from '../../api/shipping.service';
import { HikeError } from '../../errors/HikeError';

export interface UseSearchShippingLabelsOptions
  extends Omit<UseQueryOptions<ShippingLabel[], HikeError<null>>, 'queryKey' | 'queryFn'> {
  query: string;
  queryKey?: QueryKey;
}

export const useSearchShippingLabels = ({ query, queryKey = [], ...options }: UseSearchShippingLabelsOptions) =>
  useQuery({
    queryKey: ['searchShippingLabels', query, queryKey],
    queryFn: async () => await findShippingLabels(query),
    ...options
  });