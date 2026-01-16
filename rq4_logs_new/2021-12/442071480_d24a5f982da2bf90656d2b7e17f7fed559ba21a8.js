const bcrypt = require('bcrypt');
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

exports.agregaVecino = async (req, res, next) => {
    try {
        let {
            nombre,
            primerapellido,
            segundoapellido,
            cedula,
            fallecido,
            fechanac,
            casa,
            email,
            trabaja,
            telefono
        } = req.body;

        if(
            fallecido != null && 
            casa != null && 
            trabaja!= null && 
            nombre != null && 
            primerapellido != null && 
            cedula != null
            )
            {
            fallecido = parseInt(fallecido);
            casa = parseInt(casa);
            trabaja = parseInt(trabaja);
            fechanac = new Date(fechanac);
        
        

            const vecino = await prisma.vecino.create({
                data:{
                    nombre: nombre,
                    primerapellido: primerapellido,
                    segundoapellido: segundoapellido,
                    cedula: cedula,
                    fallecido: fallecido,
                    fechanac: fechanac,
                    casa: casa,
                    email: email,
                    trabaja: trabaja,
                    telefono: telefono
                }
            });
            res.json(vecino);
        }

    }catch(e){
        console.log(e.message);
        res.json({mensaje: "Ocurrio un error"+e.message});
        next();
    }
}

exports.obtieneVecinos = async (req, res, next) => {
    try {

        const vecino = await prisma.vecino.findMany();
        res.json(vecino);

    }catch(e){
        console.log(e.message);
        res.json({mensaje: "Ocurrio un error"+e.message});
        next();
    }
}