var fs= require("fs")

// console.log(fs)
// let data="";
// let readStream=fs.createReadStream("Avinash.txt")
// // console.log(readStream)
// readStream.on("data",function(x){
//     data+=x
//     console.log(data)
// })

// readStream.on("end",function(){
//     console.log(data)
// })


let readStream=fs.createReadStream("Avinash.txt")

let writeStream=fs.createWriteStream("output.txt")

readStream.pipe(writeStream)
