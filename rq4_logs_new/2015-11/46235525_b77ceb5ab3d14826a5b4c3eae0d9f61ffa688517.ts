namespace classessample  {
  "use strict";

  class MostSimpleClass { }

  class Greeter {
      constructor(private greeting = "Hello, ") { }
      greet(name: string) {
          console.log(this.greeting + name);
      }
  }

  new Greeter("Hallo, ").greet("Karlsruhe");
  new Greeter().greet("Karlsruhe");

  console.log(`typeof Greeter: ${typeof Greeter}`); // function
  console.log(`typeof new Greeter(): ${typeof new Greeter()}`); // object
  console.log(`Greeter: ${Greeter}`); // the constructor method


  class ClassWithOwnProperties {
      age: number;
      name: string;
      constructor(age, name) {
          this.age = age;
          this.name = name;
      }
  }

  class ClassWithConstructorProperties {
      constructor(public age: number, public name: string) { }
  }

  class ClassWithDefaultConstructorProperties {
      constructor(public age: number = 0, public name: string = randomString()) { }
  }

  function randomString() {
      return "this is random.";
  }
}


namespace classeswithfunctionssample {
  "use strict";

  class SomeClass {
    addOneRegular (a: number) {
      return a + 1;
    }

    addOneArrowed = (a: number) => {
      return a + 1;
    };

    public addOneRegularPublic (a: number) {
      return a + 1;
    }

    private addOneRegularPrivate (a: number) {
      return a + 1;
    }

    // omit function keyword inside classes
    addOneAsFunction = function (a: number) {
      return a + 1;
    };
  }

  var someInstance = new SomeClass();
  console.log(someInstance.addOneRegular(2));
  console.log(someInstance.addOneArrowed(3));

  console.log(someInstance.addOneRegularPublic(4));
  console.log(someInstance.addOneAsFunction(5));

  // will work when compiled to js, but compile error in ts
  // console.log(someInstance.addOneRegularPrivate(6));
}