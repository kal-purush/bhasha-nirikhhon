import { Request, Response } from "express";
import { Product } from "../../models";
const errors = require("../errors");

const isTest = true;//ATTENTION!!!! REMOVE!

const productControllers = {

    create: async (request: Request, response: Response) => {
        
        const {
            dimensions,
            weight,
            name,
            SKU,
            category
        } = request.body;
        //NEEDS TO RECEIVE E img FROM THE REQUEST
        //USE THE FACTORY TO DEAL WITH THE IMAGE EXTENSIONS
        try {
            const DBResponse = await Product.create({
                dimensions,
                weight,
                name,
                SKU,
                category
            }); 
            //IDEALY, WE WILL DEAL WITH THE IMAGES HERE
            if(isTest) console.log(DBResponse);
            return response.sendStatus(200);
        } catch (error) {
            if(isTest) console.log(error);
            return response.status(500).json(errors.internal_server_error);
            
            
        }  
       
    },

    findOne: async (request: Request, response: Response) => {
            
        const { id } = request.params;
        try {

            const DBResponse = await Product.findById(id);

            if(!DBResponse) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);
         
        } catch (error) {
            
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);
        }
    },

    findByCategory: async (request: Request, response: Response) => {

            const { category } = request.params;

        try {
            
            const DBResponse =  await Product.find({ "category.name": category });
            
            if(!DBResponse.length) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            return response.status(500).json(errors.internal_server_error);                       
        }
    },

    findByName: async (request: Request, response: Response) => {

            const { name } = request.params;

        try {
        
            const DBResponse =  await Product.find({ name: name });            
            
            if(!DBResponse) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            return response.status(500).json(errors.internal_server_error);
           
        }
    },

    findAll: async (request: Request, response: Response) => {

        try {
            
            const DBResponse = await Product.find();
            
            if(!DBResponse) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }

    },

    update: async (request: Request, response: Response) => {
        
        const { id } = request.params
        const { 
                name, 
                SKU, 
                dimensions, 
                weight, 
                category, 
                stock 
            } = request.body;

        try {

            const DBResponse = await Product.updateOne(
                {
                    _id: id
                }, 
                {
                    $set:{
                        name, 
                        SKU, 
                        dimensions, 
                        weight, 
                        category, 
                        stock
                    }
                }
            );

            return response.status(204).json(DBResponse);
       
            
        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }

    },

    delete: async (request: Request, response: Response) => {

        const { id } = request.params;

        try {
            
            const DBResponse = await Product.findByIdAndDelete(id);

            return response.sendStatus(204)


        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);  
        }
        
    },
}

export default productControllers;