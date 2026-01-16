var express = require('express');
var router = express.Router();
const UsuarioController = require('../controllers/UsuarioController.js');

router.get('/', UsuarioController.index);
router.get('/edit/:id', UsuarioController.edit);
router.post('/store', UsuarioController.validation, UsuarioController.store);
router.put('/update/:id', UsuarioController.update);
router.post('/delete/:id', UsuarioController.destroy);

module.exports = router;