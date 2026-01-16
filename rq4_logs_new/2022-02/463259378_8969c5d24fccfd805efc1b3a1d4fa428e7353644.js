// extends

// - Class는 extends를 이용하여 상속받을 수 있다. 
// - 자식 클래스가 부모 클래스를 상속받아서 부모클래스의 기능을 물려받아 사용할 수 있다.

// 부모함수를 참조할 때 super()를 사용

// - Overriding a method : 부모의 함수를 받아서 재정의해서 사용할 수 있다.
// - 자신의 함수가 없으면 부모의 함수를 실행

// -  자바스크립트는 단일상속만을 허용 => 클래스는 클래스 하나만 상속받을 수 있다.


class Animal {

    constructor(name) {
      this.speed = 0;
      this.name = name;
    }
  
    run(speed) {
      this.speed = speed;
      console.log(`${this.name} runs with speed ${this.speed}.`);
    }
  
    stop() {
      this.speed = 0;
      console.log(`${this.name} stands still.`);
    }
  
  }
  
  class Rabbit extends Animal {
    hide() {
        console.log(`${this.name} hides!`);
    }
  
    stop() {
      super.stop(); // call parent stop
      this.hide(); // and then hide
    }
  }
  
  let rabbit = new Rabbit("White Rabbit");
  
  rabbit.run(5); // White Rabbit runs with speed 5.
  rabbit.stop(); // White Rabbit stands still. White Rabbit hides!







  // question

  class Animal {
    constructor(name) {
      this.name = name;
    }
    // ...
  }
  
  class Rabbit extends Animal {
  
    constructor(name, earLength) {
      this.name = name;
      this.earLength = earLength;
    }
  
    // ...
  }
  
  let rabbit = new Rabbit("White Rabbit", 10); // Error : this.name 은 상속받았기 때문에 super(name) 을 사용해야 함






// // solution

class Animal {

    constructor(name) {
      this.name = name;
    }
  
    // ...
  }
  
  class Rabbit extends Animal {
  
    constructor(name, earLength) {
      super(name);
      this.earLength = earLength;
    }
  
    // ...
  }
  
  let rabbit = new Rabbit("White Rabbit", 10);
  console.log(rabbit.name); // White Rabbit
  console.log(rabbit.earLength); // 10















// class Mother{
//     constructor(name, age){
//         this.name = name;
//         this. age = age;
//     }
//     getInfo(){
//         return console.log('내이름은'+this.name+'나이는'+this.age+'살이야');
//     }
// }


// class Child extends Mother{
// }


// let a = new Mother('뽀로로',7);
// a.getInfo();


// let b = new Child('크롱',29);
// b.getInfo();


// console.log(a)
// console.log(b)