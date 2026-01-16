module.exports = function asyncReduce(array,fn,initValue){
  return Array.prototype.reduce.call(array,function(pre,cur,index,_arrary){
    return pre.then(function(_pre){
      return fn(_pre,cur,index,_arrary);
    })
  },Promise.resolve(initValue));
}