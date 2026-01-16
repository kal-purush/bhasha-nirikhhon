import { useContext } from 'react';
import { FORM_CONTEXT } from './formContext';
import type { FormCtxVal, Values } from './types';

export default function useFormActions() {
  const { actions } = useContext(FORM_CONTEXT) as FormCtxVal<Values>;

  return { ...actions };
}