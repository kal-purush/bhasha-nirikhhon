const { body } = require('express-validator');
const {compareSync} = require('bcryptjs');
const { all } = require('../models/usuario');

const login = [
    body('email').notEmpty().withMessage('Debe escribir su E-mail').bail().isEmail().withMessage('Formato de E-mail inválido').bail().custom( value => {
        let usuariosRegistrados = all();
        let usuarios = usuariosRegistrados.map( usuario => usuario.email);

        if(!usuarios.includes(values)){
            throw new Error('El E-mail no se encuentra registrado');
        };
        return true;
    }),
    body('password')
    .notEmpty()
    .withMessage('Ingrese su contraseña')
    .bail()
    .custom((value, {req}) =>{
        let usuariosRegistrados = all();
        let usuarios = usuariosRegistrados.find(u => u.email === email);
        if(!usuarios){
            throw new Error('Usuario sin registrar');
        }
        if(!compareSync(value, usuarios.password)){
            throw new Error('La contraseña es incorrecta');
        };
        return true;
    })

]


module.exports = login;