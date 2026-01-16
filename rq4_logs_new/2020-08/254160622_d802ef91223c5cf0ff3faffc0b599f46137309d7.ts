// TODO: add disabledUI props
// export interface DisabledUIProps {
//   hasDisabledUI?: boolean;
// }

// https://www.w3schools.com/tags/tag_input.asp
export interface InputProps {
  ariaPressed?: boolean | "mixed" | undefined;
  ariaLabel?: string | undefined;
  className?: string;
  placeholder?: string;
  type:
    | "color"
    | "hidden"
    | "image"
    | "file"
    | "month"
    | "range"
    | "search"
    | "week"
    | "text"
    | "number"
    | "date"
    | "datetime-local"
    | "email"
    | "password"
    | "search"
    | "tel"
    | "url"
    | "week";
  required?: boolean;
  disabled?: boolean;
  name: string;
  value: string | string[] | undefined | number;
  label: string;
  tabIndex?: string;
  maxLength?: number;
  pattern?: string;
  errorText?: string;
  icon?: React.ReactNode;
  size?: "large" | "medium";
  hasError?: boolean;
  isValid?: boolean;
  onInputChange: (value: string | number) => void;
}

export interface WidthProps {
  width?: number | string;
}

export interface StyledLabelProps {
  hasIcon?: string | number | true | undefined;
  disabled?: boolean;
  hasError?: boolean;
  hasFocus: boolean;
}

export interface LabelContainerProps {
  hasFocus: boolean;
  hasIcon: boolean;
  disabled: boolean;
  size: number;
}