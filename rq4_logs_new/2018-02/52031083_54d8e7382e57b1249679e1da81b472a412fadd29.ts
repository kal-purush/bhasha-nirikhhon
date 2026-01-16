import { Writer } from "../types";
import { StructuredWriter } from "./structured";

export class JsonValueWriter extends StructuredWriter {
  writeFloat64(value: number): number | string {
    if (isNaN(value)) {
      return "NaN";
    } else if (value === Infinity) {
      return "+Infinity";
    } else if (value === -Infinity) {
      return "-Infinity";
    } else if (Object.is(value, "-0")) {
      return "-0";
    }
    return value;
  }

  writeDate(value: Date): string {
    return value.toISOString();
  }

  writeNull(): null {
    return null;
  }

  writeBuffer(value: Uint8Array): string {
    const result: string[] = new Array(value.length);
    const len: number = value.length;
    for (let i: number = 0; i < len; i++) {
      result[i] = (value[i] < 16 ? "0" : "") + value[i].toString(16);
    }
    return result.join("");
  }

  writeBoolean(value: boolean): boolean {
    return value;
  }

  writeString(value: string): string {
    return value;
  }

  writeMap(
    size: number,
    keyHandler: <KW>(index: number, mapKeyWriter: Writer<KW>) => KW,
    valueHandler: <VW>(index: number, mapValueWriter: Writer<VW>) => VW,
  ): any {
    // TODO: Use a specialized writer that only accepts strings and numbers (KeyMustBeAStringError)
    // Let users build custom serializers if they want
    const jsonWriter: JsonValueWriter = new JsonValueWriter();
    const result: any = {};
    for (let index: number = 0; index < size; index++) {
      const key: any = keyHandler(index, jsonWriter);
      result[JSON.stringify(key)] = valueHandler(index, this);
    }
    return result;
  }
}