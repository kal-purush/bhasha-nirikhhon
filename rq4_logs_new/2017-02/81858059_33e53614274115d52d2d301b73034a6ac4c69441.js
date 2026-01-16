
  var arr=[]
  var arrr=[]
  var displayArr =[]
  var someOtherArray=[]
  var someOtherArray2=[]
  var opernsArr=[]

  
document.querySelector('.calculator-AC').addEventListener('click',function(event){
  // var target = event.target
  document.querySelector('.calculator-display').innerHTML=0})




document.querySelector('.calculator').addEventListener('click', clickButton)

function clickButton(event) {
  var target = event.target

  //if AC is clicked clear Calculator display
  if(target.innerHTML == 'AC'){
    document.querySelector('.calculator-AC').addEventListener('click',function(event){
      // var target = event.target
      document.querySelector('.calculator-display').innerHTML=0})
  } else {
    displayArr.push(target.innerHTML)
    console.log(displayArr);
    document.querySelector('.calculator-display').innerHTML= displayArr.join('')
  }
}
document.querySelector('.calculator').addEventListener('click', arr1)

// /if operator is clicked concat array
// then create next array
// then click equal and concat second array
// perform calculation
//
//
// array 1


function arr1(){
  var target = event.target
console.log('new',arr)
  if(Number.isInteger(parseInt(target.innerHTML))&&(!!someOtherArray)){
    arr.push(target.innerHTML)

    }else if(target.dataset.opperator){

     someOtherArray.push( arr.join(''))
     console.log(arr.join(''))
     console.log(typeof arr.join(''))
     console.log(typeof arr)


 }
}
//convert operns to an array
document.querySelector('.calculator').addEventListener('click',function(){
  var target = event.target
  // var opperator=['+','-','\/','%','+/-']
  if(target.dataset.opperator){
    opernsArr.push(target.innerHTML)
    console.log('operns', opernsArr)
  }
 })


// for(var i = 0;i < opperator.length; i++){
//   do{event.target.innerHTML = opperator}
//   while(opperator== displayArr[i])
// }
//
// }

// array 2
document.querySelector('.calculator').addEventListener('click', arr2)

  function arr2(){
    var target = event.target
  console.log('new2',arrr)
    if(Number.isInteger(parseInt(target.innerHTML))&&(!!someOtherArray2)){
      arrr.push(target.innerHTML)

   }else{
     if(target.dataset.opperator){
       someOtherArray2.push( arrr.join(''))
       console.log(arrr.join(''))
       console.log(typeof arrr.join(''))
       console.log(typeof arrr)
     }

   }

  }


// if(!arr){
//
//   arrr()
// }else arrr()
// // var calc=(function createCalculator(someOtherArray,someOtherArray2){
// //                 var a = someOtherArray
// //                 var b = someOtherArray2
// //         return{
// //                 mult:function(a,b){return a*b},
// //                 div:function(a,b){return a/b},
// //                 add:function(a,b){return a+b},
// //                 sub:function(a,b){return a-b}
// //         }
// // })()
// // calc.add(20,20);
// // calc.sub(20,20);
// // calc.div(20,20);
// // calc.multi(20,20);
//
// // //loop through array of opperator and match it to displayArr
// // function opperator(){
//   var opperator=['+','-','\/','%','+/-']
//
//   for(var i = 0;i < opperator.length; i++){
//     do{event.target.innerHTML = opperator}
//     while(opperator== displayArr[i])
//   }
//
// }
// // //match displayArr to calc.multi
// // //split number into arrA[0] and arrB[2]
// //
// // displayArr.split(operator)
// //
// //
// //
// // var calc=(function createCalculator(a,b){
//
//         return{
//                 mult:function(a,b){return a*b},
//                 div:function(a,b){return a/b},
//                 add:function(a,b){return a+b},
//                 sub:function(a,b){return a-b}
//         }
// })()
// document.querySelector('.calculator-display').innerHTML= calc.mult
// //
// // ////////
// //
// //
// //
// //
// // function  evalButton(event){
// //   evaluArr=[]
// // document.querySelector('.calculator-equal').addEventListener('click',function(event){
// //     evaluArr.push(displayArr.slice())
// //     for(var i = 0; i < evaluArr.length; i++){
// //       if( typeOf(i) == Number ){evalArr.join('').parsInt
// //     }
// //
// //       }
// //           evaluArr.join('').reduce(function(num1, (opperator+num2)) {
// //             return num1 (opperator+ num2);
// //       }),0);
// //     }
// //   })
// // }
// //
// // // Evaluating expression/ function for = button
// // // displayArr.slice.push (evalArr)
// // // displayArr.split('')
// // // if typeOf is a number.join
// // // then reduce by specified operator and return to .calculator display/displayArry
// //
//
// // //////////////other solution
// // var calc=(function createCalculator(){
// //         return{
// //                 mult:function(a,b){return a*b},
// //                 div:function(a,b){return a/b},
// //                 add:function(a,b){return a+b},
// //                 sub:function(a,b){return a-b}
// //         }
// // })()
// //
// //
// const arrays = [[],[]]
//  var index1 = 0
// var x=[ '2', 'X', '3' ]
//
// for(let a of x ){
// if(parseInt(a)){
// 	arrays[index1].push(a)
// }else{
// index1++
// 	}
// }
// arrays