const express = require('express');
const router = express.Router();
const tagDescriptorController = require('../controllers/tagDescriptorController')
const auth = require('../middlewares/auth')
const {check} = require('express-validator')

router.get('/',
    auth,
    tagDescriptorController.createDocument) ;

module.exports = router