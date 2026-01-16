/*****************************************
 *        🤓 ISAAC ESTEVE AQUI 🤓       *
 *****************************************/

import { Request, Response } from 'express'
import { Categorie } from "../../models";
const errors = require("../errors");

const isTest = true;//ATTENTION!!!! REMOVE!

const catergorieControllers = {

    create: async (request: Request, response: Response) => {
        
        const newCategorie = request.body;

        try {
            const DBResponse = await Categorie.find({ name: newCategorie.name }).count();
            //By demand, we need to check that no category is duplicated
            if(DBResponse) return response.status(403).json(errors.forbidden);
           
            await Categorie.create({...newCategorie});
            if(isTest) console.log("New Category created!");
            return response.sendStatus(200);           
        } catch (error) {
            if(isTest) console.log(error);
            return response.status(500).json(errors.internal_server_error);
        }

    },
    
    findOneByName: async (request: Request, response: Response) => {
        const { name } = request.params;

        try {
            const DBResponse = await Categorie.findOne({ name: name });

            if(!DBResponse) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse); 

        } catch (error) {
            return response.status(500).json(errors.internal_server_error);
        }
    },

    findAll: async (request: Request, response: Response) => {
        try {
            
            const DBResponse = await Categorie.find();
            
            if(!DBResponse.length) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }        
    },
    update: async (request: Request, response: Response) => {
        const { name } = request.params;
        const toUpdate = request.body;
        try {
            
            const DBResponse = await Categorie.findOneAndUpdate(
                {
                    name: name
                },
                {
                    $set:{
                       ...toUpdate
                    }
                },
                {
                    new: true
                });
            
            if(!DBResponse) return response.status(404).json(errors.not_found);

            return response.status(200).json(DBResponse);

        } catch (error) {
            if(isTest) console.log(error);
            response.status(500).json(errors.internal_server_error);            
        }     
    }

}

export default  catergorieControllers;