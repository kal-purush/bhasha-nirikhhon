'use client';
import { useMemo } from 'react';
import { useWindowWidth } from '@react-hook/window-size';
import type { Nullable } from '../typeHelpers';

/**
 * From globals.css, but these match Tailwind current def
 */
const breakpoints = {
  sm: 40,
  lg: 64,
};

const remSize = (() => {
  if (typeof document === 'undefined') return 16;
  const html = document?.querySelector('html');
  return html ? parseFloat(getComputedStyle(html).fontSize) : 16;
})();

export const useMasonry = () => {
  const width = useWindowWidth();
  const widthToBreakpoint = width / remSize;
  return useMemo(() => {
    const columns =
      widthToBreakpoint >= breakpoints.lg
        ? 3
        : widthToBreakpoint >= breakpoints.sm
          ? 2
          : 1;

    const getItems = <T>(items: Nullable<Array<T>>) => {
      if (!items) return;
      return items.reduce(
        (acc, item, i) => {
          const index = i % columns;
          acc[index]?.push(item);
          return acc;
        },
        // This uses Array.from syntax because we have to initiate a new array for each element
        // .fill([]) reuses the same array so the reference persists
        Array.from({ length: columns }, () => []) as Array<T[]>,
      );
    };

    return {
      getItems,
      columns,
    };
  }, [widthToBreakpoint]);
};