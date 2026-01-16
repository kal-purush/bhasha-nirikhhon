import type { IconName } from "@/app/(ui)/input";
import type { DualIcon } from "@/app/types.index";
import type { LoginSchema, LoginTypeSchema } from "@/server/resource/account";
import type { FormEvent, HTMLInputTypeAttribute } from "react";

export type LoginProps = {
  signinType: LoginTypeSchema;
};

import type {
  Control,
  ControllerRenderProps,
  UseFormReturn,
} from "react-hook-form";

export type SubmitButtonProps = {
  signinType: LoginTypeSchema;
  isValid: boolean;
  loading: boolean;
};

export type TField = {
  field: ControllerRenderProps<LoginSchema>;
};

export type LoginName = "email" | "password";

export type FormType = UseFormReturn<LoginSchema>;

export type LoginFormProps = {
  signinType: LoginTypeSchema;
  form: FormType;
  loading: boolean;
  onSubmit: (values: LoginSchema) => void;
};

export interface LoginField {
  name: LoginName;
  label: string;
  placeholder: string;
  inputType: "input" | "select";
  type: HTMLInputTypeAttribute;
  alt: IconName;
  icon: DualIcon;
}

export type FormFieldProps = {
  fields: LoginField[];
  control: Control<LoginSchema>;
};

// LOBBY

export type FormProps = {
  loginType: LoginTypeSchema;
  setLoginType: (type: LoginTypeSchema) => void;
};

export type FormEventButtonClick = FormEvent<HTMLButtonElement>;
export type FormEventSubmit = (e: FormEventButtonClick) => void;