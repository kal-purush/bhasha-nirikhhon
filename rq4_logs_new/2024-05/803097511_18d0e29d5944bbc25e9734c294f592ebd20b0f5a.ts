import { CorsOptions } from "cors" 


export const corsConfig : CorsOptions = {
    origin: function(origin, callback){
        // console.log(process.argv)//Son argumentos, parametros que se ejecutan con el scrip
        
        const whiteList = [process.env.FRONTEND_URL]
        if(process.argv[2] === "--api"){   
            whiteList.push(undefined)
        }
        if(whiteList.includes(origin)){
            callback(null, true)
        } else{
            callback(new Error("Error de cors"), false)
        }
    }
} 