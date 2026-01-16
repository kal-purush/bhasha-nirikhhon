// Objeto de Conexión
const sequelize = require('../config/seq')

// DataTypes 
const { DataTypes , ValidationError } = require ('sequelize')

// Modelo A Trabajar 
const CourseModel = require ('../models/courses')

// Crear el objeto Course 
const Course = CourseModel(sequelize, DataTypes)


// Get: Obtener datos -  Read
exports.traerCourses = async(req, res)=>{
    try {
        const courses = await Course.findAll();
        res.status(200).json(
            {
                "success" : true,
                "data" : courses
            }
        )        
    } catch (error) {
        //Errores de Servidor
        res
        .status(500)
        .json({
            "success":false,
            "errors": error
        })  
    }

}

// Obtener recurso por id
exports.traerCoursesPorId = async(req, res)=>{
    try {
        const courseId = await Course.findByPk(req.params.id)
        //Si el course no existe
        if (!courseId) {
            res.status(422).json(
                {
                    "success" : false,
                    "data" : [
                        "Course no Existe"
                    ]
                }
            )     
        }else{
        res.status(200).json(
            {
                "success" : true,
                "data" : courseId
            }
        )   
        }
    } catch (error) {
        //Errores de Servidor
        res
        .status(500)
        .json({
            "success":false,
            "errors": "Error del Servidor"
        })  
    }
}

// Post: Crear un nuevo recurso - Create 
exports.crearCourse = async(req, res)=>{
    try {
        const newCourse = await Course.create(req.body);
        res.status(201).json({
            "success" : true,
            "data" : newCourse
        })
    } catch (error) {
        if(error instanceof ValidationError){
            //Mensajes de Errores
            const errores = error.errors.map((e)=> e.message )
            // Llevar errores al response 
            res
            .status(422)
            .json({
                "success":false,
                "errors": errores
            })
        }else{
            //Errores de Servidor
            res
            .status(500)
            .json({
                "success":false,
                "errors": "Error del Servidor"
            })
        }
    }   
}

//Put - Patch : Actualiza un recurso - Update
exports.actualizarCourse = async(req, res)=>{
    try {
        // Consultar Datos Actualizados
        const upCourse = await Course.findByPk(req.params.id)
        if (!upCourse) {
            res.status(422).json(
                {
                    "success" : false,
                    "data" : [
                        "Course no Existe"
                    ]
                }
            )     
        }else{
            // Actualizar Course Por Id
            await Course.update(req.body,{
            where:{
                id: req.params.id
                }
            });
            const upAct = await Course.findByPk(req.params.id)
        res.status(200).json(
            {
                "success" : true,
                "data" : upAct
    
            }
        )
        }
    } catch (error) {
        if(error instanceof ValidationError){
            //Mensajes de Errores
            const errores = error.errors.map((e)=> e.message )
            // Llevar errores al response 
            res
            .status(422)
            .json({
                "success":false,
                "errors": errores
            })
        }else{
            //Errores de Servidor
            res
            .status(500)
            .json({
                "success":false,
                "errors": "Error del Servidor"
            })
        }
    }

}
//Delete : Elimina un recurso - Delete
exports.borrarCourse = async(req, res)=>{
    try {
        // Consultar Datos A Eliminar
        const u = await Course.findByPk(req.params.id)
        if (!u) {
            res.status(422).json(
                {
                    "success" : false,
                    "data" : [
                        "Course no Existe"
                    ]
                }
            )     
        }else{
            // Buscar al usuario
            const u = await Course.findByPk(req.params.id)
            // -Borrar usuario por id
            await Course.destroy({
            where: {
              id: req.params.id
            }
            });
            res.status(200).json(
                {
                    "success" : true,
                    "data" : u
                }
            )
        }
    } catch (error) {
        //Errores de Servidor
        res
        .status(500)
        .json({
            "success":false,
            "errors": "Error del Servidor"
        })  
    }              
}