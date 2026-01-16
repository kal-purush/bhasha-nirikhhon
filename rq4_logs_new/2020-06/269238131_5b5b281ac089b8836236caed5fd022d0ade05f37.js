et mongoose = require('mongoose');
let Pintores = require('../models/Pintores');

let PintoresController = {};

/*LISTAR*/
PintoresController.list = (req, res)=>{
    Pintores.find({})
        .limit(20)
        .skip(0)
        .exec((err, pintor)=>{
            if (err){
                console.log('Error:',err);
            }
            //console.log('Datos obtenidos');
            //console.log(estatal);
            res.render('../views/pintores/index',{
                Pintores: Pintor,
                title: "Lista de pintores",
                year: new Date().getFullYear()
            });
        })
};

module.exports = PintoresController;