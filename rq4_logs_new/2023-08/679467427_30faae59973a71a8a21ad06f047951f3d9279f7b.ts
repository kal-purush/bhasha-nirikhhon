const {User} =  require("../../../../../BACK/MVP_BACK/src/models/User");
const bcrypt = require("bcryptjs");
import { Request, Response } from "express";


const AuthController = {
    
    async login(req: Request, res: Response){
        const {email, password} = req.body

        const usuarios = await User.findOne({
            where: {
              email,
            },
        });

        if(!usuarios){
            return res.status(400).json("email invalido!")
        }
        if(!bcrypt.compareSync(password, usuarios.password)){
            return res.status(401).json("password invalido!");
        }

        return res.json("Logado");
    },



};

export default AuthController;