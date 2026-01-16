

function concat() {
  var returnString ="";
for (var i=0; i < arguments.length;i++) {
  returnString= returnString + arguments[i];
}
  console.log(returnString);
  return returnString;
}
console.log(concat("six ","degrees"," of ","separation"));

function endsWith (searchString,inString){
  var searchLen = searchString.length;
  console.log("search length " + searchLen);
  var stringLen = inString.length;
  console.log("string length" + stringLen);
  var startSearch = stringLen - searchLen;
  console.log("startSearch at " + startSearch);
   if (searchString===inString.substr(startSearch, searchLen))
{
     return true;
   }
     else {return false;}
}

console.log(endsWith("six", "endsinsix"));
console.log(endsWith("five", "endsinsix"));

function included (searchString,inString){

var found = inString.match(searchString);
console.log(found);
if (found)
{
console.log("I found "+ searchString + " in " + inString);
return true;
}
else {
  console.log("I did not find "+ searchString + " in " + inString);
  return false;
}
}

included("isItThere","YesisItThereIsHere");
included("isItThere","NoThisIsDifferent");
// str.indexOf(searchValue[, fromIndex])
var start=0;

function myIndexOf (searchString,inString,start){
  var midString= inString.substr(start, inString.length);
  console.log('Index of String ""' + searchString + '" is "' + (midString.search(searchString)) + ' in the string ""'+ midString + '');
  return (midString.search(searchString));
}

myIndexOf("isItThere","YesisItThereIsHere");
myIndexOf("isItThere","NoThisIsDifferent");

myIndexOf("isItThere","YesisItThereIsHere",20);

myArray=[10,20.3,50];

function push(myArray,newValue){
  myArray[myArray.length]=newValue;
  console.log(myArray);
}

push(myArray,"test");
push(myArray,"23");

function aConcat(array1,array2){
  for (i=0; i < array2.length; i++){
    array1[array1.length]=array2[i];
  }
  console.log(array1);
  return array1;
}
aConcat(myArray,[1,12,25]);

function aJoin(myArray,separator){
  var myString="";
  for (i=0; i < myArray.length; i++){
    myString=myString + myArray[i];
    if (separator != null && i < (myArray.length - 1)){
      myString=myString + separator;
    }

    console.log(myString);
  }
  console.log(myString);
  return myString;
}
aJoin([1,67,88,44,99],',');
aJoin([1,67,88,44,99]);