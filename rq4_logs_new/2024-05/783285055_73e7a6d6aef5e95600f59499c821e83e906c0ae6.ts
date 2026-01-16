'use client';

import { ChangeEvent, useCallback, useMemo, useRef } from 'react';
import { chain } from '@react-aria/utils';
import { As, ClassNamesType } from '@/types';
import { cn } from '@/utils';
import useDOMRef from '@/hooks/useDOMRef';
import useDisclosure from '@/hooks/useDisclosure';
import useOutsideClick from '@/hooks/useOutsideClick';

export interface UseDropdownProps {
  as?: As;
  onChange?: (value: string) => void;
  classNames?: ClassNamesType<'base' | 'item' | 'menu'>;
}

export function useDropdown({ ...props }: UseDropdownProps) {
  const { as, classNames, onChange } = props;
  const { isOpen, onOpenChange, onOpen, onClose } = useDisclosure();

  const Component = as || 'div';

  const ref = useRef<HTMLDivElement>(null);
  const domRef = useDOMRef(ref);
  const buttonRef = useRef<HTMLButtonElement>(null);

  useOutsideClick(domRef, onClose);

  const handleChange = useCallback(
    (e: ChangeEvent<HTMLButtonElement>) => {
      onChange?.(e.target.value);
    },
    [onChange],
  );

  const context = useMemo(
    () => ({
      isOpen,
      onOpenChange,
      onClose,
      onOpen,
    }),
    [isOpen, onOpenChange, onOpen, onClose],
  );

  const getBaseProps = useCallback(
    () => ({
      ref: domRef,
      className: cn('relative', classNames?.base),
    }),
    [domRef, classNames?.base],
  );

  const getMenuProps = useCallback(
    () => ({
      className: cn(
        'absolute top-27 w-full z-[10] rounded-3 dropdown-blur bg-[white/0.85]',
        isOpen && 'border-1 border-[white] shadow-drodpown',
        classNames?.menu,
      ),
    }),
    [classNames?.menu, isOpen],
  );

  const getItemButtonProps = useCallback(
    () => ({
      ref: buttonRef,
      onClick: chain(handleChange, onClose),
      className: cn(classNames?.item),
    }),
    [buttonRef, classNames?.item, onClose, handleChange],
  );

  return {
    Component,
    getBaseProps,
    getItemButtonProps,
    getMenuProps,
    context,
  };
}

export type UseDropdownReturn = ReturnType<typeof useDropdown>;