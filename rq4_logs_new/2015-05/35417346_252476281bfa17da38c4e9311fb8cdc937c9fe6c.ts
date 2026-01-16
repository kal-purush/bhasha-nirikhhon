/**
 * Created by cash on 2015/05/12.
 */

// 基本泛型

//function Hello(num:number):number{
//    return num;
//}

//使用 any
//function Hello(str:any):any{
//    return 100;
//}

// <T>

//function Hello<T>(arg:T):T{
//    return arg;
//}
//
//var output = Hello<string>("Hello Cash");
//
//alert(output);

// 泛型的運用

// 沒有 length 屬性
//function Hello<T>(num:T):T{
//    alert(num.length);
//    return num;
//}


//function Hello<T>(str:T[]):T[]{
//    //alert(str.length);
//    return str;
//}
//
//var list:Array<string> = Hello<string>(["1", "2", "3"]);
//
//for(var i = 0; i<list.length; i++)
//{
//    alert(list[i]);
//}


// 泛型類型

//function Hello<T>(arg:T):T{
//    return arg;
//}
//
//var myHello : <T>(arg:T) => T = Hello;
//alert(myHello("hello"));

//另一種寫法

//function Hello<T>(arg:T):T{
//    return arg;
//}
//
//var myHello:{<T>(arg:T):T} = Hello;
//
//alert(myHello("Hello"));

//使用 interface

//interface Hello {
//    <T>(arg:T):T;
//}
//
//function myHello<T>(arg:T):T{
//    return arg;
//}
//
//var MH:Hello = myHello;
//alert(MH<string>("Hello"));
//
//alert(MH<number>(1200));

//使用 interface2，將泛型指定拉回 interface

//interface Hello<T>{
//    (arg:T):T;
//}
//
//function myHello<T>(arg:T):T{
//    return arg;
//}
//
//var mh : Hello<number> = myHello;
//
//alert(mh(100));


// 泛型 class

class HelloNumber<T>{
    Ten:T;
    add:(x:T, y:T) => T;
}

var myHelloNumber = new HelloNumber<number>();
myHelloNumber.Ten = 10;
myHelloNumber.add = function(x, y){
    return x + y;
};

alert(myHelloNumber.Ten);
alert(myHelloNumber.add(10, 10));
//---
var myHelloNumber2 = new HelloNumber<string>();
myHelloNumber2.Ten = "Hello";
myHelloNumber2.add = function(x, y){
    return x + y;
};

alert(myHelloNumber2.Ten);
alert(myHelloNumber2.add("hello", myHelloNumber2.Ten));