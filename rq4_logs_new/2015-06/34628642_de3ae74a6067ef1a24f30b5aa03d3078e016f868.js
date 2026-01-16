//---------------------------
//  Simple Async performance
//---------------------------

function now(){
  return 4;
}

function later(){
  answer = answer * 2
  console.log("the meaning of life:" answer)
}

var answer = now();

setTimeout(later,1000)