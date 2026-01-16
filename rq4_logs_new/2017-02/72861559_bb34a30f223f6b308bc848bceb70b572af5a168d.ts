import * as glob from 'glob';
import * as fs from 'fs';


glob("**/*",{
    ignore:"node_modules"
},(err:Error,files:string[])=>{


    files= files.filter(file=>{
        return /\.d\.ts$/.test(file) || /\.js$/.test(file);
    }).filter(file=>{
        return !/node_modules/.test(file)
    }).filter(file=>{
        return !/^scripts/.test(file);
    }).filter(file=>{
        return ~file.indexOf("/")
    })
    
    function next(err?:Error){
        if(err)throw err;
        const file = files.shift()
        if(file){
            console.log(file);
            let timeout = /^Framework/.test(file)?10:500;
            fs.unlink(file,next);
        }else{
            console.log("Completed");
        }
    }


    next();
})