const Utils={};
const ANALOG_REF=5;

Utils.LM35ToCelsius=function(raw){
     var value=ANALOG_REF* 1000*raw/1024
     
     return value/10;
}





module.exports=Utils;