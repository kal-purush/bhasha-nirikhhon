import { TypeDef } from "..";
import { green, red, magenta, cyan } from "colors";

abstract class AbstractExample {}
class Example extends AbstractExample {}
class DeriveExample extends Example {}

function example() {}
const arrow = () => {};
const symbol = Symbol("test");

interface Interface {}
namespace Interface {
  export const $TypeName = "Interface";
  export declare const $Type: Interface;
}

describe("TypeDef.of", () => {
  it("class", () => {
    expect(TypeDef.of(Example)).toBe(Example);
    expect(TypeDef.of(DeriveExample)).toBe(DeriveExample);
    expect(TypeDef.of(AbstractExample)).toBe(AbstractExample);
    expect(TypeDef.of(new Example)).toBe(Example);
    expect(TypeDef.of(new DeriveExample)).toBe(DeriveExample);
  });

  it("function", () => {
    expect(TypeDef.of(arrow)).toBe(arrow);
    expect(TypeDef.of(example)).toBe(example);
  });

  it("interface", () => {
    expect(TypeDef.of(Interface)).toBe(Interface);
    expect(TypeDef.of({}, Interface)).toBe(Interface);
  });

  it("symbol", () => {
    expect(TypeDef.of(symbol)).toBe(symbol);
  });

  it("string", () => {
    expect(TypeDef.of("test")).toBe("test");
  });
});

describe("TypeDef.name", () => {
  it("class", () => {
    expect(TypeDef.name(Example)).toBe(Example.name);
    expect(TypeDef.name(DeriveExample)).toBe(DeriveExample.name);
    expect(TypeDef.name(AbstractExample)).toBe(AbstractExample.name);
  });

  it("function", () => {
    expect(TypeDef.name(arrow)).toBe(arrow.name);
    expect(TypeDef.name(example)).toBe(example.name);
  });

  it("interface", () => {
    expect(TypeDef.name(Interface)).toBe(Interface.$TypeName);
  });

  it("symbol", () => {
    expect(TypeDef.name(symbol)).toBe(symbol.toString());
  });

  it("string", () => {
    expect(TypeDef.name("test")).toBe("test");
  });
});

describe("TypeDef.is", () => {
  const row = [AbstractExample, Example, DeriveExample, example, arrow, symbol, Interface];
  const is: Map<any, Map<any, boolean>> = new Map();
  for (const r of row) is.set(r, new Map());

  const IS = cyan("is ");
  const NOT = magenta("not");

  for (const a of row)
    describe(TypeDef.name(a), () => {
      for (const b of row) {
        const explicit = is.get(a).get(b);
        const expected = explicit !== void 0 ? explicit : a === b
          || isPrototypeOf(b, a)
          || false;
        const msg = [expected ? IS : NOT, TypeDef.name(b)].join(" ");
        it(msg, () => expect(TypeDef.is(a, b)).toBe(expected));
      }
    });
});

function isPrototypeOf(a: any, b: any): boolean {
  if (a && a.prototype && b && b.prototype)
    return a.prototype.isPrototypeOf(b.prototype);
}