import { useState, useCallback } from 'react';

export function useFormAndValidation(inputValues: { [key: string]: string }) {
  const defaultValues = inputValues;
  const [values, setValues] = useState<{ [key: string]: string }>(defaultValues);
  const [errors, setErrors] = useState<{ [key: string]: string }>({});
  const [isValid, setIsValid] = useState<boolean | undefined>(true);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    if (e) {
      const { name, value } = e.target;
      setValues({ ...values, [name]: value });
      setErrors({ ...errors, [name]: e.target.validationMessage });
      setIsValid(e.target.closest('form')?.checkValidity());
    }
  };

  const resetForm = useCallback(
    (newValues = defaultValues, newErrors = {}, newIsValid = false) => {
      setValues(newValues);
      setErrors(newErrors);
      setIsValid(newIsValid);
    },
    [setValues, setErrors, setIsValid],
  );

  return { values, handleChange, errors, isValid, resetForm, setValues, setIsValid };
}