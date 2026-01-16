namespace castingsample {
  "use strict";

  let known: string = "really a string";
  let probably: any = known;

  let unknown: any;
  let itsAString: string = unknown;
  let itsANumber1: number = unknown;
  let itsANumber2 = <number>unknown;
  let itsANumber3 = unknown as number;

  let numValTypedAny: any = 0;
  let b = numValTypedAny as string;
  let c = <string>numValTypedAny;
  let d: string = numValTypedAny;

  let someString = "i'a a string";
  // let number1 = <number>someString; // error
  // let number2: number = someString; // error
  let number3: number = <any>someString;

  function funWithStringReturn() {
    return "i'm a string return";
  };

  // let number4: number = funWithStringReturn(); // error
  let number5: number = <any>funWithStringReturn();

  // override inferred return type
  function funWithAnyReturn(): any {
    return 5;
  }

  let iKnowItsAString: string = funWithAnyReturn();
}