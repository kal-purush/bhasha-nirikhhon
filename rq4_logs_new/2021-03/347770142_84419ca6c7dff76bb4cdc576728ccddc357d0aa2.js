const User = require('../models/User')

const bcryptjs = require('bcryptjs');

class UserController {

    constructor(dao){
        this.dao = dao;
    }

    index = async (req, res) => {

        const users = await this.dao.find();
        return res.status(201).json(users)
    }

    create = async (req, res) => {
        const { login, password } = req.body;

        try{

            if(await this.dao.findOne({ login: login, active: true }))
                return res.status(400).json({ return: false, message: 'Usuário já cadastrado.' });

            if(password){

                const hash = await bcryptjs.hash(password, 10);

                req.body.password = hash;

                const user = await this.dao.create(req.body);

                delete user.password;

                return res.status(201).json({ return: true, data: user })
            }else{
                return res.status(400).json({ return: false, message: 'Preencha a senha.' });
            }
            
        }catch (err){
            return res.status(400).json({ return: false, message: 'Ocorreu um erro: ' + err });
        }
    }
}

module.exports = new UserController(User);