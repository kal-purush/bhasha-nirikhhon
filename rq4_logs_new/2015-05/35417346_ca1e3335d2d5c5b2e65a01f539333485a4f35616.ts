/**
 * Created by cash on 2015/05/12.
 */


//最簡單的參數限制

//function printLabel(labelObj:{label:string}){
//    console.log(labelObj.label);
//}
//
//var myObj = {label:"Hello"};
//
//printLabel(myObj);

// 使用 interfacce 來聲明

//interface LabelValue{
//    label:string;
//}
//
//function printLabel(labelObj:LabelValue){
//    console.log(labelObj.label);
//}
//
//var myObj = { label :"Hello-1"};
//
//printLabel(myObj);

// 可選屬性

//interface USB{
//    name?:string;
//    age?:number;
//}
//
//function printUSB(pu:USB){
//    console.log(pu.name);
//    //console.log(pu.age);
//}
//
//var my = { name : "cash" };
//
//printUSB(my);

// function 類型

//interface SearchFunc {
//    (source:string, subString:string):boolean;
//}
//
//var mySearch:SearchFunc;
//mySearch = function(src:string, sub:string){
//    var result = src.search(sub);
//
//    if(result != -1){
//        return true;
//    }
//    else{
//        return false;
//    }
//}

// array 類型

//interface StringArray{
//    [index:number]:string;
//}
//
//var myArray:StringArray;
//myArray = ["cash", "eee", "if"];
//alert(myArray[1]);


// class 類型

//interface ClockInterface{
//    currentTime:Date;
//    setTime(d:Date);
//}
//
//class Clock implements ClockInterface {
//    currentTime:Date;
//    setTime(d:Date){
//        this.currentTime = d;
//    }
//    constructor(h:number, m:number){
//
//    }
//}

// 接口繼承

interface Shape{
    color:string;
}

interface PenStroke{
    penWindth: number;
}

interface Square extends Shape, PenStroke{
    sideLength:number;
}

var s = <Square>{};

s.color = "blue";
s.sideLength = 10;
s.penWindth = 11;

//混合類型

interface Counter {
    interval:number;
    reset():void;
    (start:number):string;
}

var c:Counter;
c(10);
c.reset();









