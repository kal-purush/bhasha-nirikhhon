import React, { createContext, useContext } from 'react';

export interface InputGroupState extends React.HTMLAttributes<HTMLInputElement> {
  radius?: 'standard' | 'round';
}

const InputGroupContext = createContext<InputGroupState>({ radius: 'standard' });

export const useInputGroupContext = (): any => {
  const context = useContext(InputGroupContext);
  if (!context) {
    throw new Error(`InputGroup compound components cannot be rendered outside the InputGroup component`);
  }
  return context;
};

export { InputGroupContext };