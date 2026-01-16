//数组去重有多种方法，现在先列举一下

var str = [1,1,14,5,2,'1',1];

//1、用ES6的Set
var st0 = Array.from(new Set(str));

//2、Set + 扩展运算符
var st1 = [...new Set(str)];


//3、使用Map

function unique1(arr) {
    var seen = new Map();   //此处之所以不用对象，是因为对象的键只能用字符串··，1 和 '1' 无法辨别
     var str =  arr.filter(function(item){
        return seen.has(item)?false:(seen.set(item,true));  //第一次出现的直接加到seen中，后面再出现的就被过滤了···
    })
   return str;
}

//4、使用最基本的算法··
function unique2(arr) {
    
    return  arr.filter(function(item,index,arr) {
        return arr.indexOf(item) === index;          //只保留首次出现的··
    }) 
}


//5、先排好序，再比较
function unique3(arr) {
    return arr.concat().sort().filter(function(item,index,arr){
        return item !== arr[index+1];   //从第一项开始依次与后一项比较，若不等则通过过滤, 若是纯数字数组倒是无所谓
                                        //如果其中有类似'1' 和 1 这样的就会出bug,因为sort 会让'1' == 1; 所以不建议使用这个·
    })
}

console.log(unique1(st0))





