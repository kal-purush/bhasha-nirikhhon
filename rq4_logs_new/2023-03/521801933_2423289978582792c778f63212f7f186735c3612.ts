import type * as ktypes from "./kantuTypes";

// Utils for converting JS representations of
// Kantu types (e.g., Nat, List)
// to and from their native JS analogues (e.g., number, Array).

export function toArr<T>(list: ktypes.List<T>): T[] {
  if (list.length === 2) {
    return [];
  }
  if (list.length === 4) {
    return [list[2]].concat(toArr(list[3]));
  }
  throw { badList: list };
}

export function intToNum(int: ktypes.Int): number {
  if (int[0] === "neg") {
    return -posToNum(int[1]);
  }
  if (int[0] === "nat") {
    return natToNum(int[1]);
  }
  throw { badInt: int };
}

export function natToNum(nat: ktypes.Nat): number {
  if (nat[0] === "zero") {
    return 0;
  }
  if (nat[0] === "pos") {
    return posToNum(nat[1]);
  }
  throw { badNat: nat };
}

export function posToNum(pos: ktypes.Pos): number {
  return parseInt(posToBitString(pos), 2);
}

export function posToBitString(pos: ktypes.Pos): string {
  if (pos[0] === "one") {
    return "1";
  }
  if (pos[0] === "extend") {
    const left = posToBitString(pos[1]);
    const right = String(bitToNum(pos[2]));
    return left + right;
  }
  throw { badPos: pos };
}

export function bitToNum(bit: ktypes.Bit): number {
  if (bit[0] === "zero") {
    return 0;
  }
  if (bit[0] === "one") {
    return 1;
  }
  throw { badBit: bit };
}

export function numToNat(n: number): ktypes.Nat {
  if (n < 0) {
    throw { cannotConvertNegNumToNat: n };
  }
  if (n === 0) {
    return ["zero"];
  }
  return ["pos", numToPos(n)];
}

export function numToPos(n: number): ktypes.Pos {
  if (!Number.isInteger(n) || n < 0) {
    throw { cannotConvertNonNumToPos: n };
  }
  if (n === 1) {
    return ["one"];
  }
  return ["extend", numToPos(Math.floor(n / 2)), numToBit(n % 2)];
}

export function numToBit(n: number): ktypes.Bit {
  if (n === 0) {
    return ["zero"];
  }
  if (n === 1) {
    return ["one"];
  }
  throw { cannotConvertNumToBit: n };
}

export function toKString(s: string): ktypes.KString {
  const textEncoder = new TextEncoder();
  const buff = new Uint8Array(s.length * 3);
  const encodedResults = textEncoder.encodeInto(s, buff);
  const utf8 = Array.from(buff.subarray(0, encodedResults.written)).map(toU8);
  return ["utf8", toList(utf8)];
}

export function toJsString(s: ktypes.KString): string {
  const bytes = new Uint8Array(toArr(s[1]).map(u8ToNum));
  const decoder = new TextDecoder();
  return decoder.decode(bytes);
}

export function toU8(n: number): ktypes.U8 {
  if (n < 0 || n > 255) {
    throw { cannotConvertNumToU8: n };
  }
  return ["u8", numToNat(n), {}];
}

export function u8ToNum(u8: ktypes.U8): number {
  return natToNum(u8[1]);
}

export function toList<T>(arr: T[]): ktypes.List<T> {
  if (arr.length === 0) {
    return ["nil", typePlaceholder()];
  }
  return ["cons", typePlaceholder(), arr[0], toList(arr.slice(1))];
}

export function typePlaceholder(): {} {
  return {};
}

export function miscPlaceholder(): {} {
  return {};
}