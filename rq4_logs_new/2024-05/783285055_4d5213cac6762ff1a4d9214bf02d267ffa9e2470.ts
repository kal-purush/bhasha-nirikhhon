'use client';

import { ChangeEvent, useCallback } from 'react';
import { As, ReactRef } from '@/types';
import { cn } from '@/utils';
import { TextareaAutosizeProps } from 'react-textarea-autosize';

export interface UseTextAreaProps extends TextareaAutosizeProps {
  as?: As;
  ref?: ReactRef<HTMLTextAreaElement>;
  onValueChange?: (value: string) => void;
}

export function useTextArea({ ...props }: UseTextAreaProps) {
  const { as, ref, onChange, onValueChange, ...otherProps } = props;

  const Component = as || 'div';

  const handleValueChange = useCallback(
    (e: ChangeEvent<HTMLTextAreaElement>) => {
      onValueChange?.(e.target.value);
      onChange?.(e);
    },
    [onValueChange, onChange],
  );

  const getBaseProps = useCallback(
    () => ({
      ref,
    }),
    [ref],
  );

  const getInputProps = useCallback(
    () => ({
      ...otherProps,
      onChange: handleValueChange,
      className: cn('resize-none outline-none w-full', props.className),
    }),
    [handleValueChange, props.className, otherProps],
  );

  return { Component, getBaseProps, getInputProps };
}