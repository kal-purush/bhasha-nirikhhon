import { Request, Response } from "express"
import { TurmaDatabase } from "../database/TurmaDatabase"
import { Turma } from "../models/Turma"
import { v4 as generateId } from 'uuid';

export const createTurma = async (req: Request, res: Response) => {
    let errorCode = 400
    try {
        const {nome, modulo} = req.body

        if (!nome || !modulo) {
            throw new Error("Body inválido.")
        }

        // const newProduct: Product = {
        //     id: Date.now().toString(),
        //     name,
        //     price
        // }

        const newClass = new Turma(
            generateId (),
            nome,
            modulo
        )

        // await connection(TABLE_PRODUCTS).insert({
        //     id: product.getId(),
        //     name: product.getName(),
        //     price: product.getPrice()
        // })
        const turmaDatabase = new TurmaDatabase()
        await turmaDatabase.create(newClass)
        
        res.status(201).send({ message: "Turma Criada", newClass: newClass })
    } catch (error) {
        res.status(errorCode).send({ message: error.message })
    }
}