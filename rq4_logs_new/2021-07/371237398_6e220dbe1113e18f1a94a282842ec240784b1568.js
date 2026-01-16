//Mongo
const mongoose = require('mongoose');
const producto = require('./models/carrito');
const config = require('./config/config.json');
//Mysql
const options = require('./config/database');
const knex = require('knex')(options.mysql);


const ClassMongo = class Mongo{
    constructor(url){
        (async () => { 
            await mongoose.connect(url, { useNewUrlParser: true, useUnifiedTopology: true });
         })()  
    }
    async insertarCarrito (item){ 
        try{
            return await producto.create(item);
        }
        catch(error){
            return {Error:"No se pudo agregar item al carrito", Description: error}
        }
    }
    async listarCarrito(){ 
        try{
             return await producto.find({});
            }
            catch(error){
                return {Error:"No se puden listar items del carrito", Description: error}
            } 
    }
    async buscarCarrito(id){ 
        try{
        return await producto.findById(id);
        }
        catch(error){
            return {Error:"No se encontro item del carrito", Description: error}
        }
    }

    async eliminarCarrito(id){ 
        try{
        return await producto.deleteOne({ _id: id});
        }
        catch(error){
            return {Error:"No se pudo eliminar item del carrito", Description: error}
        }
    }
}

const ClassMySQL =class MySql{
    constructor(){}

    //Insert
    async insertarCarrito (item){  
        try{
            let producto = {timestamp_carrito: item.timestamp_carrito, producto: JSON.stringify(item.producto)}
            return await knex('carrito').insert(producto)         
        }       
        catch(error){
            return {Error:"No se pudo agregar item al carrito", Description: error}
        }
    }
    //select
    async listarCarrito(){ 
        try{ 
        return await knex.from('carrito').select('*')
        }
        catch(error){
            return {Error:"No se puden listar items del carrito", Description: error}
        } 
    }
    //Select por id
     async buscarCarrito(id){ 
        try{ 
        let resultado= await knex.from('carrito').select('*').where('id', parseInt(id))
        if (resultado.length<1)
        throw {Error:`No se encontro producto ${id}`};
        return resultado;    
    }
        catch(error){
            return {Error:"No se encontro item del carrito", Description: error}
        }
    }
    
    //Borrar producto
    async eliminarCarrito(id){ 
        try{ 
       let resultado=await knex.from('carrito').where('id', parseInt(id)).del()
        if (resultado==0)
        throw {Error:`No se encontro item ${id} para eliminar`};
        return {Estatus:`Se borro item con id ${id}`};
        }
        catch(error){
            return {Error:"No se pudo eliminar item del carrito", Description: error}
        }
    }
}

module.exports = {ClassMongo, ClassMySQL}