import { Test } from 'beater-reporter';

const named = (name: string, t: Test): Test => {
  Object.defineProperty(t, 'name', { value: name });
  return t;
};

const name = (t: Test): string => t.name;

export { name, named };