import { Request, Response } from "express";
import addUser from "../data/addUser";

export default async function createUser(
    req: Request,
    res: Response
) {
    try {

        const { name, email, password } = req.body
        const id :string = Date.now() + Math.random().toString()

        if (!name || !email || !password) {
            res
            .status(400).send("preencha corretamente os campos 'name', 'email' e 'password' ")
        }

        await addUser(id, name, email, password)

        res
        .status(201)
        .send({
            message:"Usuário cadastrado com sucesso",
            id:id
        })

    } catch (error: any) {

        const duplicate:string = error.sqlMessage

        if(duplicate&&duplicate.indexOf("Duplicate")>= 0){
            res.status(400).send("Email ja cadastrado.")
        }
        res
        .status(500)
        .send({
            message:error.message|| error.sqlMessage
        })
    }
}