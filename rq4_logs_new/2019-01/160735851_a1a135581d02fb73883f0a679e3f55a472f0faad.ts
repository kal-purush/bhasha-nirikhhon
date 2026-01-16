/** All the types that can be found in the form. */
export type ValueType = string | boolean | number | undefined;

export interface Form {
  [key: string]: ValueType | Form;
}

/**
 * The validation error messages.
 * Key is the field name.
 * Value is the error message.
 */
export interface FormErrors {
  [key: string]: string;
}

/** State of the form component. */
export interface FormState {
  /** The field values. */
  form: Form;

  /** Raised errors. */
  errors: FormErrors;

  /** Form has been submitted. */
  submitSuccess?: boolean;
}