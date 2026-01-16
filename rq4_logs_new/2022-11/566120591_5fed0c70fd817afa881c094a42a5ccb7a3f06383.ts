import { type Direction, type Edge, BinaryValue, Gpio, Options} from "onoff";

export type ComponentOptions = Options;

export class Component extends Gpio {
  constructor(
    gpio: number,
    direction: Direction,
    edge?: Edge,
    options?: ComponentOptions
  ) {
    super(gpio, direction, edge, options);
  }

  write(
    value: BinaryValue | boolean,
    callback: (err: Error | null | undefined) => void
  ): void;
  write(value: BinaryValue | boolean): Promise<void>;
  write(
    value: boolean | BinaryValue,
    callback?: (err: Error | null | undefined) => void
  ): void | Promise<void> {
    value = Number(value) as BinaryValue;
    if (callback) {
      return super.write(value, callback);
    }

    return super.write(value);
  }
}