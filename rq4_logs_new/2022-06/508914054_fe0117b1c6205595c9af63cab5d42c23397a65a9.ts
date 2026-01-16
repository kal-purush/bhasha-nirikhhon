import { UserModel } from './../database/models/UserModel';
import { Request, Response } from "express";

class UserController {

    async findAll(req:Request, res:Response) {
        const users = await UserModel.findAll();
        try{
            if(users.length > 0){
                return res.status(200).json(users);
            }
        } catch(e) {
            return res.status(204).json(e);
        }
    }

    async findOne(req:Request, res:Response) {
        const { id } = req.params;
        const user = await UserModel.findOne({
            where: {
                id: id
            }
        });

        try{
            if(user){
                return res.status(200).json(user);
            }
        } catch(e) {
            return res.status(204).json(e);
        }
    }

    async create(req:Request, res:Response) {
        const { name, email, password } = req.body;
        const user = await UserModel.create({
            name,
            email,
            password
        });

        return res.status(201).json({name: name, email: email});
    }

    async update(req:Request, res:Response) {
        const { id } = req.params;
        await UserModel.update(req.body, {
            where: {
                id: id
            }
        });

        return res.status(200).send();
    }

    async destroy(req:Request, res:Response) {
        const { id } = req.params;
        const user = await UserModel.destroy({
            where: {
                id: id
            }
        });

        return res.status(200).send();
    }


}

export default new UserController();