var fs = require('fs');
// const data =fs.readFileSync('input.txt');  // 同步读取文件api

// fs.readFile('input.stxt',function(error,data){ // 异常情况
fs.readFile('input.txt',function(error,data){
  if(error){
  console.log(error);
  }else{
    console.log(data.toString());
  }
});

console.log('程序执行结束');