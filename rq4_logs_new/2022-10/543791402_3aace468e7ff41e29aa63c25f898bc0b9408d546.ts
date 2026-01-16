import { Request, Response } from "express";
import { EstudanteDatabase } from "../database/EstudanteDatabase";

export const searchEstudante = async (req: Request, res: Response) => {
    let errorCode = 400
    try {
        let nome = req.params.nome
        const estudanteDatabase = new EstudanteDatabase
        console.log(nome)
        
        if(!nome){
            throw new Error("Favor informar um nome");            
        }
        
        const aluno = await estudanteDatabase.getByName(nome)      
        
        res.status(201).send({message: "Aluno encontrado", aluno: aluno})

    } catch (error) {
        res.status(402).send({message: "Aluno não encontrado"})
    }
}