
var a :string="Pakistan";
console.log("hello world" +a);
a="usa";
a= <any> 123;
console.log("a var is change to any "+a);
var b=false;
if(b==false){
	let b:string="Pakistan zinda bad";
	console.log(b);
	
}else{
	let b:string="india morda bad";
	console.log(b);
}
interface LabelledValue {
  label: string;
}

function printLabel(labelledObj: LabelledValue) {
  console.log(labelledObj.label);
}

var myObj = {size: 10, label: "Size 10 Object"};
printLabel(myObj);