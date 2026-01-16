import { Request, Response } from "express";
import User, { IUser } from '../models/user';

export async function getUsers(req:Request, res:Response){
    const  users = await User.find();
    if(!users) return res.status(404).json({message:"Error al obtener los ususarios"});

    return res.status(200).json(users);
}
export async function getUser(req:Request,res:Response){
    const {id} = req.params;
    const user= await User.findById(id);
    if(!user) return res.status(404).json({message:"No se ha encontrado el usuario"});
    return res.status(200).json(user);
}
export async function changeRole(req:Request, res:Response){
    const {id}= req.params;
    const {userRole}=req.body;
    const update= await User.findByIdAndUpdate(id,{
        userRole
    });
    if(!update) return res.status(404).send({message:"No se ha encontrado el usuario"});
    return res.status(200).json(update);
}