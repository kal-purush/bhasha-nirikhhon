import { ChangeEvent } from "react";

export interface IInput {
  type: "search" | "default";
  placeholder?: string;
  handleChange?: (text: string) => void;
  handleChangeEvent?: (e: ChangeEvent<HTMLInputElement>) => void;
  name?: string;
  className?: string;
}