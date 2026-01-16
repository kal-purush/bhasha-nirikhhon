import { useRouter } from 'next/navigation';
import { useEffect, useMemo, useReducer } from 'react';

import { useAtom } from 'jotai';

import { getGradientBorderStyle } from '@/shared/utils/get-gradient-border-style';

import { useFiltersContext } from '@/filters/providers/filters-provider/context';

const TOGGLE_TEXT = 'Filters & Sorting';
const ACTIVE_BUTTON_STYLE = getGradientBorderStyle();
const EXCLUDED_KEY = 'query';
const RANGE_KEYS_REGEX = /min|max/g;

export const useFilterToggler = () => {
  const {
    initializedFilters,
    isPendingFilters,
    atom,
    startFiltersTransition,
    routeSection,
  } = useFiltersContext();

  const [atomValue, setAtom] = useAtom(atom);

  const filterCount = useMemo(() => {
    const keys = new Set();
    atomValue.forEach((_, k) => {
      if (k !== EXCLUDED_KEY) keys.add(k.replace(RANGE_KEYS_REGEX, ''));
    });
    return keys.size;
  }, [atomValue]);

  const [isOpen, toggleOpen] = useReducer((prev) => !prev, false);

  useEffect(() => {
    if (isOpen && isPendingFilters) {
      toggleOpen();
    }
  }, [isOpen, isPendingFilters]);

  const toggleStyle =
    (isOpen || filterCount > 0) && !isPendingFilters
      ? ACTIVE_BUTTON_STYLE
      : undefined;
  const buttonText = `${TOGGLE_TEXT}${filterCount > 0 ? ` (${filterCount})` : ''}`;

  const { push } = useRouter();

  const applyFilters = () => {
    startFiltersTransition(() => {
      push(`/${routeSection}?${atomValue.toString()}`);
    });
  };

  const clearFilters = () => {
    startFiltersTransition(() => {
      setAtom(new URLSearchParams());
      push(`/${routeSection}`);
    });
  };

  const isDisabledApply = filterCount === 0;
  const isDisabledClear = atomValue.size === 0;

  return {
    initializedFilters,
    isPendingFilters,
    toggleOpen,
    toggleStyle,
    buttonText,
    isOpen,
    applyFilters,
    clearFilters,
    isDisabledApply,
    isDisabledClear,
  };
};