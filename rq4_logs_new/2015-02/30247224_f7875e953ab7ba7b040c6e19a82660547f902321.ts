// コンストラクターでsuperキーワードを使用する場合は、構文が異なる
class A {
    constructor(public x: number) {
    }
}
class B extends A {
    constructor(public x: number, public y: number) {
        super(x);
    }
}
var n = new B(123, 456);
console.log(n.x.toString());
console.log(n.y.toString());