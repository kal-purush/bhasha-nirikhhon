import jwt from "jsonwebtoken"

export const jwtVerfication=(secret)=>{

   return async(req,res,next)=>{
 

    const token = req.headers['authorization'].split(' ')[1]
       
       if(!token) return res.json({message:"token is not found ..please login"})
       else{
       
           try{

            const userdata=await jwt.verify(token,secret)
            req.userdata=userdata
            console.log('verfied succesfull')
            next()

           }catch(err){

            return res.json({message:'error occured while verifying account using jwt...token is not mathcing ',error:err})
 
           }
    
    }

    }



} 