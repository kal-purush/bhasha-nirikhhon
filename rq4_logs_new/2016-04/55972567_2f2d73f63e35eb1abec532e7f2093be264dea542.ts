/**
 * Created by LENOVO on 2016/4/11.
 */
//boolean
var flag:boolean = false;
//string
var name:string = "alvin";
//number
var age:number = 22;
// '`'中时使用${} 表达式
let str:string = `hello${name}`;

//array
var arr:string[] = ["1", '2'];
var arr1:number[] = [1, 2, 3];
var arr2:Array<boolean> = [true, false];

//元组 初始化的时候顺序必须正确,
var tuple:[string,number]= ["1", 2];//ok
//var tuple:[string,number]= [1, "2"];//error
tuple = ["1", 2]; //ok
tuple[2]=11;
tuple[2]="23";
//tuple[5]=true;//error
console.info(tuple[2].toString);
//必须每个类型都有的方法才可以调用
//console.info(tuple[2].substr(1)); //error
var tuple1:[string,number,boolean];
tuple1=['2',3,true];


//enum
enum Color {RED,GRENN,BLUE};
var co:Color=Color.BLUE;
//打印下标
console.info("枚举"+co);
//指定下标
enum Color1{RED=3,GREEN=6,BLUE=9};
var co1:Color1=Color1.RED;
var co2:Color1=Color1.BLUE;
console.info("枚举"+co1);
console.info("枚举"+co2);


