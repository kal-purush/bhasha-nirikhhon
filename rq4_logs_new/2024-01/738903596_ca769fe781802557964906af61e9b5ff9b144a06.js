
/*var data = new XMLHttpRequest();
data.open("GET","https://restcountries.com/v2/all");
data.send();
data.onload = function()
{
    var rs = JSON.parse(data.response);
    console.log(rs);
}
for(let i in rs){
    console.log(rs[i].name);
} */

function compareJSON(obj1, obj2) {
    const stringifyAndSort = (obj) => JSON.stringify(obj, Object.keys(obj).sort());
  
    return stringifyAndSort(obj1) === stringifyAndSort(obj2);
  }
  
  const obj1 = { name: "Person 1", age: 5 };
  const obj2 = { age: 5, name: "Person 1" };
  
  const areEqual = compareJSON(obj1, obj2);
  
  console.log(areEqual); // true
  