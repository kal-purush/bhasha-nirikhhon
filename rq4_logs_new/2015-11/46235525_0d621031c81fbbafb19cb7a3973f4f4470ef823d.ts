namespace decoratedsample {
  "use strict";

  var log: MethodDecorator = function(target: Function, key: string, descriptor: any) {
    var originalMethod = descriptor.value;

    // this MUST not be an arrow function due to reference to "this"
    descriptor.value =  function (...args: any[]) {
        var printableArgs = args.map(a => JSON.stringify(a)).join();
        // call to original method
        var result = originalMethod.apply(this, args);
        var pritableResult = JSON.stringify(result);
        console.log(`Call: ${key}(${printableArgs}) => ${pritableResult}`);
        return result;
    };
    return descriptor;
  };


  class DecoratedClass {
    constructor(public a: number) {}

    @log
    foo(greeting: string) {
      return greeting + this.a;
    }
  }

  var greeting = new DecoratedClass(13).foo("Greetings, ");
  console.log(greeting);
}