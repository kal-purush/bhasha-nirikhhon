import { injectable } from "inversify";
import orderService from "../services/orderServices";
import { Request, Response } from "express";

@injectable()
class OrderController{
    public orderService;
    constructor(orderService:orderService){
        this.orderService=orderService;
    }

    createCustomer=async(req:Request,res:Response)=>{
          try{  
            const result=await this.orderService.createCustomer(req);
            res.status(200).send(result);}
            catch(error:any)
            {
                res.status(500).send(error.message);
            }
    }
}

export default OrderController;