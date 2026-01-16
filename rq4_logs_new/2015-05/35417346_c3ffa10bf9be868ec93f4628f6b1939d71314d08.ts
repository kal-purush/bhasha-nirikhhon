/**
 * Created by cash on 2015/05/11.
 */

// 一般的 js

//function add(x, y){
//
//    return x + y;
//}
//
//var myAdd = function(x, y){
//    return x + y;
//}

// 一般的 function

//function add(x:number, y:number):string{
//
//    return "hello typescript";
//}
//
//var myadd = function(x:number, y:string):string{
//    return "hello ts";
//};
//
//var myAddts:(name:string, age:number) =>
//    number = function(n:string, a:number) : number{
//    return a;
//};

//可選和默認參數

//function buildName(firstName?:string, lastName?:string){
//
//    if(lastName){
//        return firstName + " " + lastName;
//    }else{
//        return firstName;
//    }
//}
//
//var result1 = buildName("cash", "wu");
//var result2 = buildName("wu");
////var result3 = buildName("cash", "wu", "if");
//var result4 = buildName(); //2個都是可選的

//function buildName(firstName:string, lastName = "wu") : string{
//
//    return firstName + " " + lastName;
//}
//
//var result1 = buildName("Hello:");
//var result2 = buildName("cash", "wu");
//
//document.getElementById("pid").innerHTML = result2;
//
////var resutl2 = buildName("cash", "wu", "if");


// 可變參數

//function peopleName(firstName:string, ...restOfname:string[]){
//    return firstName + " " + restOfname.join("-");
//}
//
//var pn = peopleName("cash", "wu", "if", "?", "bean");
//
//document.getElementById("pid").innerHTML = pn;


// lamdba

// /var people = {
//    name : ["cash", "wu", "if", "bean"],
//    getName:function(){
//        return function(){
//            var i = Math.floor(Math.random()*4);
//            return {
//                n: this.name[i]
//            }
//        }
//    }
//};

//var people = {
//    name : ["cash", "wu", "if", "bean"],
//    getName:function(){
//        return () => {
//            var i = Math.floor(Math.random()*4);
//            return {
//                n: this.name[i]
//            }
//        }
//    }
//};
//
//var Myname = people.getName();
//alert("姓名: " + Myname().n);

//重載

function attr(name:string):string;
function attr(age:number):number;

function attr(nameorage:any):any{

    if(nameorage && typeof nameorage === "string" ){
        alert("姓名");
    }else{
        alert("年齡");
    }

}

attr("hello");
attr(10);







