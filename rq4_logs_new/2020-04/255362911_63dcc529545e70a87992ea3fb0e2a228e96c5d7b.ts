// eslint-disable-next-line no-unused-vars
import type { ValidationError } from 'yup';
// eslint-disable-next-line no-unused-vars
import type { Record } from 'immutable';

interface Accumulator {
  [key: string]: any,
}

interface SetFormErrorsImArgs {
  setter: React.Dispatch<React.SetStateAction<Record<Accumulator>>> & Readonly<Accumulator>,
  errors: [ValidationError],
}

export const setFormErrorsIm = ({ setter, errors }: SetFormErrorsImArgs) => {
  setter((oldState) => (
    oldState
      .clear()
      .merge(errors.reduce((acc: Accumulator, error) => {
        acc[error.path] = acc[error.path] === undefined ? error.errors[0] : acc[error.path];
        return acc;
      }, {}))
  ));
};

export default setFormErrorsIm;