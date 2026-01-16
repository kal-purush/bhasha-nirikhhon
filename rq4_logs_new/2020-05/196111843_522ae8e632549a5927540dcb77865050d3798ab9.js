import React, { useEffect, useRef } from 'react';
import ReactInputMask from 'react-input-mask';
import { useField } from '@unform/core';

import { InputGroup } from './styles';

export default function Input({ name, label, mask, ...rest }) {
  const inputRef = useRef(null);

  const { fieldName, defaultValue, registerField, error } = useField(name);

  useEffect(() => {
    registerField({
      name: fieldName,
      ref: inputRef.current,
      path: 'value',
    });
  }, [fieldName, registerField]);

  return (
    <InputGroup>
      {label && <label htmlFor={fieldName}>{label}</label>}
      {mask ? (
        <ReactInputMask
          id={fieldName}
          ref={inputRef}
          defaultValue={defaultValue}
          mask={mask}
          {...rest}
        />
      ) : (
        <input
          id={fieldName}
          ref={inputRef}
          defaultValue={defaultValue}
          {...rest}
        />
      )}
      {error && <span>{error}</span>}
    </InputGroup>
  );
}