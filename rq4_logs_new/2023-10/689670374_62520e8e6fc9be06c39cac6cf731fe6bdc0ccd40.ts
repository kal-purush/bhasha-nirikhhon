import { ChangeEventHandler } from "react";

export type InputAttributeType =
  | "text"
  | "checkbox"
  | "tel"
  | "password"
  | "email"
  | "address"
  | "time"
  | "number"
  | "date"
  | "datetime"
  | "datetime-local";

export type InputType = {
  autoComplete?: "off" | "on" | "email" | "current-password" | "new-password";
  disabled?: boolean;
  required?: boolean;
  label?: string | JSX.Element;
  maxLength?: number;
  name: string;
  onChange?: ChangeEventHandler<HTMLInputElement>;
  placeholder?: string;
  type?: InputAttributeType;
  value?: string | number | boolean;
};