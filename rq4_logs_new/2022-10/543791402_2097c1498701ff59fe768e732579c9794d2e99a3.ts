import { Request, Response } from "express";
import { EstudanteDatabase } from "../database/EstudanteDatabase";

export const editEstudanteTurma = async (req: Request, res: Response) => {
    let errorCode = 400 
    try {
        let {email, newClass} = req.body

        const estudanteDatabase = new EstudanteDatabase

        if(!email || !newClass){
            throw new Error("Dados incompletos");
        }

        let classAtu = await estudanteDatabase.getByClass(email)
        console.log(classAtu[0].turma_id)
        
        if(classAtu[0].turma_id === newClass){
            errorCode 
            throw new Error("Estudante já está na turma informada");
        }

        await estudanteDatabase.editClass(newClass, email)

        res.status(201).send({message:"Turma alterada"})
    } catch (error) {
        res.status(errorCode).send({message: error.message})
    }
}