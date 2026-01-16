import { Request, Response } from "express";
import { UserService } from "./user.service";


const createUsers =  async (req:Request, res:Response)=>{
    try {
        const userdata = req.body;
 const newusers = await UserService.CreateUserDB(userdata);

 res.status(200).json({
success:true,
message: "success",
data:newusers

 })


    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error:any) {
        res.status(500).json({
            success:false,
            message:  error.message || " not success"
        }) 
    }
}



export const UserController = {
createUsers
}