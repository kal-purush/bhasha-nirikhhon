//如果期望代码的输出变成 0 -> 1 -> 2 -> 3 -> 4 -> 5，并且要求原有的代码块中的循环和两处 console.log 不变

for (var i = 0; i < 5; i++) {
    setTimeout(function() {
        console.log(new Date, i);
    }, 1000);
}

console.log(new Date, i);

//使用
for (var i = 0; i < 5; i++) {
    (function(j) {setTimeout(function() {
        console.log(new Date, j);
    }, j*1000)})(i)
}
setTimeout(function() {
    console.log(new Date, i);
},i*1000)
//使用promise all 来解决
var tasks = []
for(var i =0; i<5; i++) {
    ((j)=> {
        tasks.push(new Promise(function(resolve) {
            setTimeout(function() {
                console.log(j);
                resolve();
    
            }, j*1000)
        }))
    })(i)
    
}

Promise.all(tasks).then(function() {
    //等输出玩 0 1 2 3 4之后 隔一秒 输出5
    setTimeout(() => {
        console.log(i)
    }, 1000);
})
