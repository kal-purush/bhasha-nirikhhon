import * as core from "@actions/core";
import { Primitive, Property } from "@k-rf/types";

export class ActionBuilder<Obj extends Record<string, unknown>> {
  getInput<K extends keyof Obj>(key: K): Obj[K] {
    return core.getInput(key as string) as Obj[K];
  }
}

export const store: {
  argName: string;
  description: string;
  defaultValue?: Primitive;
}[] = [];

export const Arg =
  ({
    description,
    defaultValue,
  }: {
    description: string;
    defaultValue?: Primitive;
  }) =>
  <T>(_target: T, fieldName: string) => {
    store.push({ argName: fieldName, description, defaultValue });
  };

export class BaseArgs<Obj> {
  private readonly ab = new ActionBuilder<Property<Obj>>();

  protected getInput<K extends keyof Property<Obj>>(key: K): Obj[K] {
    // TODO: すべて文字列で受け取るので、適切な値に変換する
    return this.ab.getInput(key);
  }
}

export * from "./template";
export * from "./generate";