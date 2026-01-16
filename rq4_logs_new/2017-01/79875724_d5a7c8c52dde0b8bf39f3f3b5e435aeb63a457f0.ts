import * as imm from "immutable";

class Apple {
  public foo(): number {
    return 3;
  }
}

class Biggie extends Apple {
  public foo(): number {
    return super.foo() + 4;
  }
}

function bar(): imm.List<number> {
  return imm.List.of(2,3,4);
}

function main(): void {
  const a = new Apple();
  console.log(a.foo());
}