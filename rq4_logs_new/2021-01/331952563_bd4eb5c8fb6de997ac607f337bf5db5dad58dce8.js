var value=0;
function kilometerToMeter(value)
{
  kilo=value*1000;
  return kilo;
}
var distance=kilometerToMeter(1);
console.log(distance);


//var mobile=100;
//var watch=50;
//var laptop=500;
function budgetCalculator(value1,value2,value3)
{
 mobile=value1*100;
 watch=value2*50;
 laptop=value3*500;

var totalAmount=mobile+watch+laptop;
return totalAmount;



}
var finalAmount=budgetCalculator(150,60,600)
console.log(finalAmount);



var rent=0;
function hotelCost(value){
if(value<=10)
{
    rent=value*100;
     
    
}
else if(value<=20)
{
 var firstTenDays= 10*100;
 var afterTenDays=value-10;
 var remaning =afterTenDays*80;
 rent=firstTenDays+afterTenDays+remaning;


}
else
{
    var firstTenDays=10*100;
    var afterTenDays=10*80;
    var remaning=value-20;
    rent=firstTenDays+afterTenDays+remaning; 
}

return rent;



}
var amount=hotelCost(14);
console.log(amount);








function megaFriend(name)
{
var max=name[0];
for(i=0;i<name.length;i++)
{
   var element=name[i];
   if(element.length>max.length){
       max=element;
   }
}
return max;



}
var output=megaFriend(['shuvo','trisha','protiva','bangladesh']);
console.log(output);