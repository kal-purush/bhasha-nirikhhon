import {NextApiRequest, NextApiResponse} from "next";
import Surreal from "surrealdb.js";

export const withSurreal = (handler: (req: NextApiRequest, res:NextApiResponse, db:Surreal )=>Promise<void>)=>{
  return async (req:NextApiRequest, res:NextApiResponse):Promise<void> =>{
    const db = Surreal(
      `http://${process.env.DB_HOST||"localhost"}:${process.env.DB_PORT||8000}/rpc`
    );
    try{
      await db.signin({
	user: process.env.DB_USER, 
	pass:process.env.DB_PASS
      });
      await db.use('spinner', 'spinner');
      await handler(req,res,db);
    }catch(e){
      console.error(e);
    }finally{
      db.close();
    }
  }
}