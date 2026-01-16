import {AbstractControl, ValidationErrors} from "@angular/forms";

export const formErrors =
  (formControls: { [key: string]: AbstractControl }): { key: string, errors: ValidationErrors }[] =>
    Object.keys(formControls)
      .reduce((errors, key) => [ { key, errors: formControls[key].errors }, ...errors ], [])
      .filter(({ errors }) => errors !== null)