import { createNotImplementedError } from "../errors/not-implemented";
import { IoType, Reader, Writer } from "../types";

export type Name = "json";
export const name: Name = "json";
export namespace json {
  export type Input = any;
  export type Output = any;
  // TODO(demurgos): Export options to JSON
  export type Type = undefined;
}
export type Diff = any;

export class AnyType<T = any> implements IoType<T> {
  readonly name: Name = name;

  constructor() {
  }

  toJSON(): json.Type {
    throw createNotImplementedError("ArrayType#toJSON");
  }

  read<R>(reader: Reader<R>, raw: R): T {
    return reader.readAny<any>(raw, {
      fromBoolean: input => input,
      fromBuffer: input => input,
      fromDate: input => input,
      fromFloat64: input => input,
      fromMap: input => input,
      fromNull: () => null,
      fromSeq: input => [...input].map((x, r) => this.read(reader, x)),
      fromString: input => input,
    });
  }

  // TODO: Dynamically add with prototype?
  write<W>(writer: Writer<W>, value: T): W {
    return writer.writeAny(value);
  }

  testError(value: T): Error | undefined {
    try {
      JSON.parse(JSON.stringify(value));
      return undefined;
    } catch (err) {
      return err;
    }
  }

  test(value: T): boolean {
    return true;
  }

  equals(val1: T, val2: T): boolean {
    // TODO: From arg
    return val1 === val2;
  }

  clone(val: T): T {
    return JSON.parse(JSON.stringify(val));
  }
}