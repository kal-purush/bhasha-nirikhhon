Array.prototype.sameStructureAs = function (other) {
    // Return 'true' if and only if 'other' has the same
    // nesting structure as 'this'.
    return deepComparison(this,other);
    // Note: You are given a function isArray(o) that returns
    // whether its argument is an array.
    
};

function deepComparison(array1,array2){
  if(array1.length !== array2.length) return false;
  for(let i = 0; i < array1.length; i++){
    if(Array.isArray(array1[i]) !== Array.isArray(array2[i])) return false;
    
    if(Array.isArray(array1[i]) && Array.isArray(array2[i])){
      if(!deepComparison(array1[i],array2[i])) return false;
    }
    
  }
  
  return true;
};