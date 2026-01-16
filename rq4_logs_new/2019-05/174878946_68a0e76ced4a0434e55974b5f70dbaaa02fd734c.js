let map04 = [
{"field":"email","node":"//*[@id='email']","operation":"INSERT"},
{"field":"gender","node":"//*[@id='gender']","operation":"INSERT"},
{"field":"ip_address","node":"//*[@id='ip']","operation":"INSERT"},
{"field":"phone","node":"//*[@id='phone']","operation":"INSERT"},
{"field":"_index","node":"//*[@id='radif']","operation":"INSERT"},
{"field":"first_name","node":"//*[@id='fname']","operation":"INSERT"},
{"field":"last_name","node":"//*[@id='lname']","operation":"INSERT"}
];
let map00 = [
{"prefunc":applyEventListener},
  {"field":"tag","node":".","operation":"INSERT"}
];
let size = 50;
function initialConstruct(){
fire(data,"//*[@id='branch']",map04,"department.user");
fire(regroup(data,size),"//*[@id='grouping']",map00,"grouping")
}
function applyEventListener(index,item,contextNode){
  contextNode.addEventListener('click',generateEventListener(index,item,size));
  contextNode.classList.add('active');
}
function generateEventListener(offset,item,size){
  let eventHandlerFunc = function(event){
    fire(nextGroup(data,offset,size),"//*[@id='branch']",map04,"department.user");
  }
  return eventHandlerFunc;
}
function nextGroup(data,offset,size){
  return data.slice(offset,offset + size);
}
function sortData(){
  data.sort(function(item1,item2){
    return item1.last_name.localeCompare(item2.last_name);
  });
  fire(data,"//*[@id='branch']",map04,"department.user");
}
function regroup(data,size){
  let division = data.length / size;
  let modulus = data.length % size;
  let result = [];
  for (var i = 1 ; i <= division ; i++)
  {
    result.push({"tag":i.toString()});
  }
  return result;
}