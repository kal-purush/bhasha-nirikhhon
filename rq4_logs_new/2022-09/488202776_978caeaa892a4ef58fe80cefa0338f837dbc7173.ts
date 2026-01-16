import { UsuarioDados } from './../data/UserDatabase';
import { Request, Response } from "express";
import { gerarToken } from "../services/gerarToken";

export default async function profileUser(req:Request,res:Response){
    try {
        const token = req.headers.authorization as string
        
        if(!token){
            res.status(400).send("token não enviado")
        }
        
        const verificaToken= new gerarToken()
        const id = verificaToken.verificadortoken(token)
        const data= new UsuarioDados
        const result = await data.getByid(id)
        const usuarioinfo = {
            id: result.id,
            email: result.email
        }

        res.status(200).send(usuarioinfo)

        

    } catch (error:any) {
        res.status(500).send(error.message||error.sqlMessage)
    }
}