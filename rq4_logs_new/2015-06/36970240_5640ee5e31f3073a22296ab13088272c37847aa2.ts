class A {
  public memberA() {
    console.log("I am in A");
  }
}

class B {
  public memberA() {
    console.log("I am in B");
  }
  public memberB;
}

var a:A;
var b:B;
b = new B();
a = b;
a.memberA();