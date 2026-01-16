import { Request, Response } from "express";
import {Estudante} from "../models/Estudante";
import { v4 as generateId } from 'uuid';
import { TurmaDatabase } from "../database/TurmaDatabase";
import { EstudanteDatabase } from "../database/EstudanteDatabase";

export const createEstudante = async (req: Request, res: Response) => {
    let errorCode = 400
    try {
        let {nome, email, dataNascimento, turmaId} = req.body
        const estudanteDatabase = new EstudanteDatabase
        
        if(!nome || !email || !dataNascimento || !turmaId){
            throw new Error("Body inválido");
        }

        const alunoExist = await estudanteDatabase.getByName(nome)
        if(alunoExist[0].length > 0) {
            throw new Error("Aluno já registrado");            
        }
        
        // const turmaIdExist = await 
        const newStudent = new Estudante(
            generateId(),
            nome,
            email,
            dataNascimento,
            turmaId
        )
        
        await estudanteDatabase.create(newStudent)

        res.status(201).send({messge: "Aluno cadastrado", newStudent: newStudent})

    } catch (error) {
        res.status(errorCode).send({message: error.message})
    }
}