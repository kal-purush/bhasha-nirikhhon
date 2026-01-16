'use client';

/* eslint-disable */

import { generateContext } from '@/react-utils';
import { Dispatch, SetStateAction } from 'react';
import { ToastOptions, ValueOrFunction } from 'react-hot-toast';

export type StrictToastRenderable = JSX.Element | string;

export const [ToastProvider, useToastContext] = generateContext<{
  toastId: string;
  setToastId: Dispatch<SetStateAction<string>>;
  handleLoading: (
    toastComponent: StrictToastRenderable,
    options?: ToastOptions,
  ) => void;
  handleError: (
    toastComponent: StrictToastRenderable,
    options?: ToastOptions,
  ) => void;
  handleSuccess: (
    toastComponent: StrictToastRenderable,
    options?: ToastOptions,
  ) => void;
  handlePromise: (
    promise: Promise<any>,
    toastComponent: {
      loading: StrictToastRenderable;
      success: ValueOrFunction<StrictToastRenderable, any>;
      error: ValueOrFunction<StrictToastRenderable, any>;
    },
    options?: ToastOptions,
  ) => void;
}>({
  name: 'toast-context',
});