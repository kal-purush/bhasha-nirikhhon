import type {
  DatePickerProps,
  InputProps,
  NumberInputProps,
  SliderProps,
  TextAreaProps,
} from '@heroui/react';
import type { Nullable } from '../../lib/utils/typeHelpers';

export interface InputQuestion extends InputProps {
  type: 'input';
  label: string;
}
export interface SliderQuestion extends SliderProps {
  type: 'slider';
  allowManual?: boolean;
}

export interface TextAreaQuestion extends TextAreaProps {
  type: 'textarea';
}

export interface DateQuestion extends DatePickerProps {
  type: 'date';
}

export interface NumberQuestion extends NumberInputProps {
  type: 'number';
}

export type Question = (
  | InputQuestion
  | SliderQuestion
  | TextAreaQuestion
  | DateQuestion
  | NumberQuestion
) & {
  id: string;
};

export const QuestionType = {
  autocomplete: 'autocomplete',
  date: 'date',
  input: 'input',
  slider: 'slider',
  textarea: 'textarea',
  number: 'number',
} as const;

export type QuestionType = (typeof QuestionType)[keyof typeof QuestionType];

export type Option = {
  label?: Nullable<string>;
  key: string;
  defaultValue?: string;
};