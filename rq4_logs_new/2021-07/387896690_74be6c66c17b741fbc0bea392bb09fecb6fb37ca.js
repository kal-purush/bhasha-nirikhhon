module.exports = function (rNumbers, startS) {
    var arr = rNumbers.split(",");
    var newArr = [];
    for (var i = 0; i < arr.length; i++) {
       var itt = arr[i];
       var trim = itt.trim();
       if (trim.startsWith(startS)) {
         newArr.push(itt);
         
       }
    }
    return newArr.length;
};