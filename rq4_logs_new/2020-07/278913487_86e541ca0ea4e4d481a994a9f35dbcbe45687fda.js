const express = require('express');
const router = express.Router();

let arrayTareas = [];

router.get('/', (req, res) => {
    res.render('index.ejs', {
        title: 'Inicio',
        tareas: arrayTareas
    });
});

router.get('/new-entry', (req, res) => {
    res.render('new_entry.ejs', {
        title: 'Nueva Entrada'
    });
});

router.post('/new-entry', (req, res) => {
    const {titulo, descripcion} = req.body;
    arrayTareas.push({
        title: titulo,
        description: descripcion,
        published: new Date()
    });

    res.redirect('/')
});
module.exports = router;

