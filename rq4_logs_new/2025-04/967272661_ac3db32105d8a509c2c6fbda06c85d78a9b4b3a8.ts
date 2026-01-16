import { useState, useCallback } from 'react';

export function useFieldStatus({ isSuccess, isFail }: { isSuccess?: boolean; isFail?: boolean }) {
  const [isFocused, setIsFocused] = useState(false);
  const [isTouched, setIsTouched] = useState(false);

  const handleFocus = useCallback(() => {
    setIsFocused(true);
  }, []);

  const handleBlur = useCallback(() => {
    setIsFocused(false);
    setIsTouched(true);
  }, []);

  const showSuccess = isTouched && isSuccess === true;
  const showError = isTouched && isFail === true;

  return {
    isFocused,
    isTouched,
    showSuccess,
    showError,
    handleFocus,
    handleBlur,
  };
}