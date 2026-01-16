console.log("HelloWorld");

console.log('%s: %d', 'Hello', 25);
var http=require("http");
http.createServer(function (req,res) {
    res.writeHead(200,{'Content-Type':"text/html"})
    res.write('<h1>Node.js</h1>');
    res.end("<p>Hello World</p>")
}).listen(3000);
console.log("HTTP server is listening at port 3000.");

/*node建一个简单的服务器*/
//1.执行脚本  node app.js
//2.访问 http://127.0.0.1:3000/


//node app.js
//实时监听修改 suervisor app.js
//默认全局对象是global,在global里定义属性就是全局属性，尽量不要使用全局属性，否则会污染其他变量
global.mymessage={"Hello":"World"};

console.log(mymessage);
//process  是一个全局变量，即  global 对象的属性。它用于描述当前 Node.js 进程状态 的对象，提供了一个与操作系统的简单接口。
console.log(process.argv);

// node process.js 1991 name=byvoid --v "Carbo Kuo"
require("./global");
console.log(mymessage);
/*util.inherits(constructor, superConstructor) 是一个实现对象间原型继承
的函数。JavaScript 的面向对象特性是基于原型的，与常见的基于类的不同。JavaScript 没有
提供对象继承的语言级别特性，而是通过原型复制来实现的*/

var util = require('util');
function Base() {
    this.name = 'base';
    this.base = 1991;
    this.sayHello = function() {
        console.log('Hello ' + this.name);
    };
}
Base.prototype.showName = function() {
    console.log(this.name);
};
function Sub() {
    this.name = 'sub';
}
util.inherits(Sub, Base);
var objBase = new Base();
objBase.showName();
objBase.sayHello();
console.log(objBase);
var objSub = new Sub();
objSub.showName();
//objSub.sayHello();
console.log(objSub);

/*注意， Sub  仅仅继承了 Base  在原型中定义的函数，而构造函数内部创造的  base  属
性和  sayHello  函数都没有被  Sub  继承。*/
/*util.inspect(object,[showHidden],[depth],[colors]) 是一个将任意对象转换
为字符串的方法，通常用于调试和错误输出。它至少接受一个参数  object ，即要转换的对象。
showHidden  是一个可选参数，如果值为  true ，将会输出更多隐藏信息。
depth  表示最大递归的层数，如果对象很复杂，你可以指定层数以控制输出信息的多
少。如果不指定 depth ，默认会递归2层，指定为  null  表示将不限递归层数完整遍历对象。
如果 color 值为  true ，输出格式将会以 ANSI 颜色编码，通常用于在终端显示更漂亮
的效果。
特别要指出的是， util.inspect  并不会简单地直接把对象转换为字符串，即使该对
象定义了 toString  方法也不会调用。*/
var util = require('util');
function Person() {
    this.name = 'byvoid';
    this.toString = function() {
        return this.name;
    };
}
var obj = new Person();
console.log(util.inspect(obj));
console.log(util.inspect(obj, true,2,true));

//node inspect.js
// Node.js 所有的异步 I/O 操作在完成时都会发送一个事件到事件队列。
var EventEmitter = require('events').EventEmitter;
var event = new EventEmitter();
event.on('some_event', function() {
    console.log('some_event occured.');
});
setTimeout(function() {
    event.emit('some_event');
}, 1000);
var fs=require("fs");
//异步读取文件
fs.readFile("testIO.txt","utf-8",function (err,data) {
    if(err){
        console.log(err);
    }else{
        //再输出这个，说明读取文件是异步的方式
        console.log(data);
    }
});
//先输出这个,这个可能有点违反直觉
console.log("end1.");


var fs=require("fs");
//异步读取文件
var data=fs.readFileSync("testIO.txt","utf-8");
//阻塞等待读取完成后，将文件的内容作为函数的返回值赋给 data 变量
console.log(data);

console.log("end1.");


var somepackage=require('./somepackage');
somepackage.Hello();

//node getPackage.js
const Hello=require("./module1").Hello;
var hello=new Hello();
hello.setName("lihua");
hello.sayHello();

// node importModules1.js
const Hello=require("./module2");

var  hello=new Hello();
hello.setName('BYVoid');
hello.sayHello();

//node importModules2.js
function Hello() {
    var name;
    this.setName = function (thyName) {
        name = thyName;
    };
    this.sayHello = function () {
        console.log('Hello ' + name);
    };
}
exports.Hello=Hello;
function Hello(){
    var name;
    this.setName = function (thyName) {
        name = thyName;
    };
    this.sayHello = function () {
        console.log('Hello ' + name);
    };
}
module.exports=Hello;
/*注意，模块接口的唯一变化是使用  module.exports = Hello 代替了  exports.Hello=
Hello 。在外部引用该模块时，其接口对象就是要输出的  Hello 对象本身，而不是原先的
exports 。*/

/*事实上， exports  本身仅仅是一个普通的空对象，即 {} ，它专门用来声明接口，本
质上是通过它为模块闭包
① 的内部建立了一个有限的访问接口。因为它没有任何特殊的地方，
所以可以用其他东西来代替，譬如我们上面例子中的 Hello  对象。*/
var a = 1;
var b = 'world';
var c = function(x) {
    console.log('hello ' + x + a);
};
c(b);
module.exports=function Hello(){
    var name;
    this.setName = function (thyName) {
        name = thyName;
    };
    this.sayHello = function () {
        console.log('Hello ' + name);
    };
}
exports.Hello=function () {
    console.log("Hello");
}
exports.Hello=function () {
    console.log("interface");
}