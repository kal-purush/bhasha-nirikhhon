import { AssetStatus } from '@hike/types';
import { QueryKey, UseQueryOptions, useQuery } from '@tanstack/react-query';
import { statsForAssets } from '../../api/asset.service';
import { HikeError } from '../../errors/HikeError';

export interface UseAssetStatsOptions
  extends Omit<UseQueryOptions<{ status: AssetStatus; count: number }[], HikeError<null>>, 'queryKey' | 'queryFn'> {
  queryKey?: QueryKey;
}

export const useAssetStats = ({ queryKey = [], ...options }: UseAssetStatsOptions = {}) =>
  useQuery({
    queryKey: ['assetStats', queryKey],
    queryFn: async () => await statsForAssets(),
    ...options
  });