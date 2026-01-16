import { WrappedFieldInputProps, WrappedFieldMetaProps } from "redux-form";
// aggregate the modules and then export
export * from "./ReduxError/ReduxError";
export * from "./ReduxInput/ReduxInput";
export * from "./ReduxTextarea/ReduxTextarea";
export * from "./untyped-field";

export interface FieldProps {
  input: WrappedFieldInputProps;
  meta: WrappedFieldMetaProps;
  formName: string;
  //TODO: be more specific with what types are allowed for the input and label properties
  inputProps: any;
  labelProps: any;
}