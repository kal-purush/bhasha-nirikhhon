'use strict'

exports.generateInitialName = (name) => {
    let split = name.split(" ");
    
    let maxArr = split.length - 1;
    let initial = "";
    
    if(split.length > 1) {
        initial = split[0][0] + split[maxArr][0];
    } else {
        initial = split[0][0];
    }

  return initial.toUpperCase();
}