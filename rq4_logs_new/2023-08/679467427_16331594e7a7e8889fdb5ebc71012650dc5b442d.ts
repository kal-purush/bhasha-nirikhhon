import { Request, Response } from "express";
import { ShoppingCart } from "../../models";
const errors = require("../errors");

const isTest = true;//ATTENTION!!!! REMOVE!

const shoppingCartControllers = {

    create: async (request: Request, response: Response) => {

        const { ownerID } = request.params;
        
        const {
            items
        } = request.body;
        
        try {
            const DBResponse = await ShoppingCart.create({
                ownerID: ownerID.toString(),
                items: items
            }); 
            if(isTest) console.log(DBResponse);
            return response.sendStatus(200);
        } catch (error) {
            if(isTest) console.log(error);
            return response.status(500).json(errors.internal_server_error);           
        }  
       
    },

    findOne: async (request: Request, response: Response) => {
            
        const { ownerID } = request.params;
        try {

            const DBResponse = await    ShoppingCart.find({
                ownerID: ownerID
            });

            if(!DBResponse.length) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);
         
        } catch (error) {
            
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);
        }
    },    

    findAll: async (request: Request, response: Response) => {

        try {
            
            const DBResponse = await ShoppingCart.find();
            
            if(!DBResponse.length) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }

    },

    update: async (request: Request, response: Response) => {
        
        const  { ownerID }  = request.params
        const { 
                items
            } = request.body;

        try {

            const DBResponse = await ShoppingCart.updateOne(
                {
                    ownerID: ownerID
                }, 
                {
                   items
                }
            );

            return response.status(204).json(DBResponse);
       
            
        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }

    },
}

export default shoppingCartControllers;