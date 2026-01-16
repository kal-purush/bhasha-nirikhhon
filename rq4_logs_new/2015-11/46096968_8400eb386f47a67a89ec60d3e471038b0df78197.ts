require("babel-polyfill")


import pinMan from "./libs/pinMan";
import readline from "./libs/readline";

let main = async ()=>{
  var con:any = await pinMan.openConnection()
  while(1){
    await readline();
    con.led.toggle();
  }
}
main();