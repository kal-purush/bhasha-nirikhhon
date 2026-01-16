 var myArray = [
     {id: 1, name: 'suyog' , age: 23}, 
     {id: 101, name: 'breast', age: 22}
    ];   
for (let i = 0; i < myArray.length; i++){
     console.log(myArray[i].name);
    }







    //for/of loop

var myArray = [
    {id: 1, name: 'khan' , age: 23}, 
    {id: 101, name: 'bhai', age: 22}
   ];   
for (let profile of myArray){
    console.log(profile.name)
   }










   //forEach()

   var myArray = [
    {id: 1, name: 'khan' , age: 23}, 
    {id: 101, name: 'bhai', age: 22}
   ];   
myArray.forEach(function(profile, index ,myArr){
    console.log('index: ${index}, Name: ${profile.name}');

});



//while loop



var myArray = [
    {id: 1, name: 'suyog' , age: 23}, 
    {id: 101, name: 'breast', age: 22}
   ];   
var i= 0;
while( i < myArray.length) {
    console.log(myArray[i].name)
    i++
   }



   //do/whileloop


   var myArray = [
    {id: 1, name: 'suyog' , age: 23}, 
    {id: 101, name: 'breast', age: 22}
   ];   
var i= 0;
do{
    console.log(myArray[i].name)
    i++
}
while( i < myArray.length) 
  

     //map()



var myArray = [
    {id: 1, name: 'suyog' , age: 23}, 
    {id: 101, name: 'breast', age: 22}
   ];   
   var newArray = myArray.map(function(profile, index, myArr){
       var newProfile = {
           'id' :index + 1,
           'fullName' : profile.firstname+' '+ profile.lastname,
           'age' : profile.age
        }

       
    return newprofile 
   })

   newArray.forEach(function(profile,index, myArray){
       console.log(profile.fullname)
   });