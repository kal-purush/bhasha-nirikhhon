import type { CompanyPreferences } from '@hike/types';
import { QueryKey, UseQueryOptions, useQuery } from '@tanstack/react-query';
import { findCompanyPreferences } from '../../api/company.service';
import { HikeError } from '../../errors/HikeError';

export interface UseCompanyPreferencesOptions
  extends Omit<UseQueryOptions<CompanyPreferences, HikeError<null>>, 'queryKey' | 'queryFn'> {
  queryKey?: QueryKey;
}

export const useCompanyPreferences = ({ queryKey = [], ...options }: UseCompanyPreferencesOptions = {}) =>
  useQuery({
    queryKey: ['session', 'companies', queryKey],
    queryFn: async () => await findCompanyPreferences(),
    ...options
  });