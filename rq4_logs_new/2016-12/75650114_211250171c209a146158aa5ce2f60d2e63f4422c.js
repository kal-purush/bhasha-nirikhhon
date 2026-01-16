// var insertionMethod = function (arr){
//
//     var checking;
//     var temp;
//     var i;
//     var x;
//
//     for (i = 1; i < arr.length; i++) {
//         checking = arr[i];
//
//         for (x = (i-1); x > -1; x--) {
//             console.log(arr);
//
//             if(checking < arr[x]){
//                 arr[x+1] = arr[x];
//             }
//         }
//         arr[x+1] = checking;
//     }
//     return arr;
// }
//


// [5,6,7,14,3,1,8,11];

function insertionMethod(input) {
 let arr = input;
 let i,j;

 for (i = 0; i < arr.length; i++) {
   let currentItem = arr[i];

   for (j = i; j > -1; j--) {
     if (arr[j] > currentItem) {
       arr[j+1] = arr[j];
       arr[j] = currentItem;
     }
   }

 }

 return arr;
}







module.exports = insertionMethod;