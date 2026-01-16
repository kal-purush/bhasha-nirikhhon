let name = 'Evan';
let age = 34;

function sayHello(nameOfPerson) {
  console.log(`Hello, my name is ${nameOfPerson}.`);
}

function sayAge(age) {
  console.log(`I am ${age} years old.`);
}

function haveBirthday(age) {
  return age + 1;
}

sayHello(name);
// => Hello, my name is Evan.
sayAge(age);
// => I am 34 years old.
age = haveBirthday(age);
sayAge(age);



class Person {
    constructor(name, age) {
      this.name = name;
      this.age = age;
    }

    sayHello() {
      console.log(`Hello, my name is ${this.name}.`);
    }

    sayAge() {
      console.log(`I am ${this.age} years old.`);
    }

    haveBirthday(age) {
      console.log(`It's my birthday!`);
      this.age += 1;
    }
  }

  let evan = new Person('Evan', 34);

  evan.sayHello();
  // => Hello, my name is Evan.
  evan.sayAge();
  // => I am 34 years old.
  evan.haveBirthday();
  // => It's my birthday.
  evan.sayAge();
  // => I am 35 years old.
  evan;