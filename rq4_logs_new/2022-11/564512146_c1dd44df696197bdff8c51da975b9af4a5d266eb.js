const express = require('express');
const api = express.Router();
const userController = require('../controllers/userController');

api.post('/user',userController.createUser);
api.get('/users',userController.getUsers);


// routas de asistentes de parvulo
api.get('/asistentes',userController.getAsistentes);
api.get('/asistente/search/:rut',userController.getAsistente);
api.get('/asistente/search',userController.getSelectionAsistentes);
api.put('/asistente/update/:rut',userController.updateAsistente);
api.delete('/asistente/delete/:rut',userController.deleteAsistente);
api.delete('/asistente/delete',userController.deleteSelectionAsistentes);


module.exports =api;