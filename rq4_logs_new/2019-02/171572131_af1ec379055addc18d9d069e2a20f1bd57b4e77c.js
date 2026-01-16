/*
CW String Incrementer
https://www.codewars.com/kata/54a91a4883a7de5d7800009c

Your job is to write a function which increments a string, to create a new string. If the string already ends with a number, the number should be incremented by 1. If the string does not end with a number the number 1 should be appended to the new string.

Examples:

foo -> foo1

foobar23 -> foobar24

foo0042 -> foo0043

foo9 -> foo10

foo099 -> foo100

Attention: If the number has leading zeros the amount of digits should be considered.

This took me a while for some reason - and I'm not 100% happy with the result.  However, I tried for a readable, functional solution and it seems to do that.
However, this code on CW used a part of the replace function I was unaware existed, which makes for a more streamlined solution - I enjoyed reading this solution and hope to use its functionality in the future
function incrementString(input) {
  if(isNaN(parseInt(input[input.length - 1]))) return input + '1';
  return input.replace(/(0*)([0-9]+$)/, function(match, p1, p2) {
    var up = parseInt(p2) + 1;
    return up.toString().length > p2.length ? p1.slice(0, -1) + up : p1 + up;
  });
}


*/



function incrementString (strng) {
  let regex = /\d+$/;
  var paddedIncrements = function() {
   //padded number holds the 0's in place as we add 1 to the #.
   //incremented number is simply incremented (needed if # before is 99, 999, etc)
   let paddedNumber = `1${(strng.match(regex))}`*1+1;
   let incrementedNumber =`${(strng.match(regex))}`*1+1;
   return (strng.match(regex)[0].charAt(0)==="9") ? incrementedNumber : `${paddedNumber}`.slice(1);
 }
return (strng.search(regex) === -1) ? strng+"1":  strng.replace(regex, (paddedIncrements()));
}



// Look how messy I was working:
// function incrementString (strng) {
//   let regex = /\d+$/;
//   console.log(strng);
//  // return (strng.search(regex) === -1) ? strng+"1": strng.replace(regex, ((strng.match(regex))*1)+1);
//
//
//  var padded = function() {
//    let number = `1${(strng.match(regex))}`*1+1;
//    console.log(strng.match(regex)[0].charAt(0));
//    console.log("HAI this is string length" +strng.match(regex)[0].length);
// //    console.log("hai this is stringnum's length" + stringnum.length);
//
//    if (strng.match(regex)[0].charAt(0)==="9") {console.log("I'M IN THE CHARAT");
//       number = `${(strng.match(regex))}`*1+1;
//       return number;
//       }
//
//      let stringnum = `${number}`;
//     console.log(stringnum)
//     return `${number}`.slice(1);
//  }
//
//
// //  console.log(typeof(((`1${(strng.match(regex))}`*1)+1)));
// // let number = `1${(strng.match(regex))}`*1+1;
// // let stringnum = `${number}.slice(1)`;
// // console.log(number);
// // console.log(stringnum);
// // console.log(typeof stringnum);
//
// // console.log("here" + `${number}`.slice(1));
// // //console.log(number.toString.slice(1));
// // console.log(((toString(`1${(strng.match(regex))}`*1))+1)).slice(0,1));
// // return (strng.search(regex) === -1) ? strng+"1": strng.replace(regex, (`${number}`.slice(1)));
// return (strng.search(regex) === -1) ? strng+"1":  strng.replace(regex, (padded()));
//
// }
//
//
// //   switch (strng.search(regex)) {
// //     case -1: return `${strng}1`;
// //     case 9:
// //     default: return strng.replace(regex, ((strng.match(regex))*1)+1);
//
// //   }
//
//
//
//  // return (strng.search(regex) === -1) ? strng+"1": strng.replace(regex, ((strng.match(regex))*1)+1);