import { InputType, ActParse } from "../types";

export const actParse =
  <T extends InputType>({
    description,
    required,
    defaultValue,
    getInput,
  }: {
    description: string;
    required?: true;
    defaultValue?: T;
    getInput: (key: string) => T;
  }): ActParse<T>["parse"] =>
  (key) => ({
    description,
    name: key,
    ...(required !== undefined && { required }),
    ...(defaultValue !== undefined && { default: defaultValue }),
    getInput: () => getInput(key),
  });