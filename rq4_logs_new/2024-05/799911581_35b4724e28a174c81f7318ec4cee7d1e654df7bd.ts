import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';

import { useAtom } from 'jotai';

import { QUERY_PARAM_KEY } from '@/filters/core/constants';
import { useFiltersContext } from '@/filters/providers/filters-provider/context';

export const useFilterQueryInput = () => {
  const {
    isPendingFilters,
    filterSearchParams,
    atom,
    initalizedQuery,
    setInitializedQuery,
    startFiltersTransition,
    routeSection,
  } = useFiltersContext();

  const [value, setValue] = useState('');

  // Initialize Query
  useEffect(() => {
    if (!initalizedQuery) {
      const initialValue = filterSearchParams.get(QUERY_PARAM_KEY) ?? '';
      setValue(initialValue);
      setInitializedQuery(true);
    }
  }, [filterSearchParams, initalizedQuery, setInitializedQuery]);

  const [atomValue, setAtom] = useAtom(atom);

  const { push } = useRouter();

  const applyQuery = () => {
    const newParams = new URLSearchParams(atomValue);

    if (value) {
      newParams.set(QUERY_PARAM_KEY, value);
    } else {
      newParams.delete(QUERY_PARAM_KEY);
    }

    setAtom(newParams);

    startFiltersTransition(() => {
      push(`/${routeSection}?${newParams.toString()}`);
    });
  };

  const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
    e.preventDefault();
    applyQuery();
  };

  const isPending = !initalizedQuery || isPendingFilters;

  return { onSubmit, isPending, applyQuery, value, setValue };
};