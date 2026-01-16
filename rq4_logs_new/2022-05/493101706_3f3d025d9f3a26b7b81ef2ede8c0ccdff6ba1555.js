//object destructuring
console.log("example 1");
let person={
    fName:"Rohan",
    lname:"S",
    age:25,
    address:{
        state:"Goa",
        pincode:123456,
    },
    hobbies:["singing","dancing","travellng","cooking"]

}
//while destructuring object key should be same
let{fName,lname,address:{state,pincode},hobbies}=person
console.log("fname",fName);
console.log("lname",lname);
console.log("state",state);
console.log("picode",pincode);
console.log("hobbies",hobbies);

console.log("example 2");
let person1={
    fName:"Shubham",
    lname:"Singh"
}
//alias name
let {fName:firstName,lname:lastName}=person1
console.log("fname",firstName);
console.log("lname",lastName);

console.log("example 3");
//array destructuring
let arr1=["singing","dancing","travelling","cooking"]
let[hob1,hob2,hob3]=arr1
console.log("hob1",hob1);