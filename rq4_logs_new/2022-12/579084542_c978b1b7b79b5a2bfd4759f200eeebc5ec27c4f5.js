const { Router } = require('express');
const { check } = require('express-validator');
const { verifyIfEmailExists, verifyIfIdExist } = require('../../helpers/db-validators');
const { validateFields } = require('../../middleware/validateFields');
const { validateJWT } = require('../../middleware/validateJWT');
const { uploadFile } = require('../../services/uploadFile');
const { getUsers, getUser, updateUser, deleteUser } = require('./usersController');

const router = Router();

router.get('/users', getUsers)

router.get('/users/:id', [
    check('id', 'The ID provided is not valid').isMongoId(),
    check('id').custom( verifyIfIdExist ),
    validateFields
], getUser)

router.put('/users/:id', [
    validateJWT,
    check('id', 'The ID provided is not valid').isMongoId().custom( verifyIfIdExist ),
    validateFields
], updateUser)

router.delete('/users/:id', [
    validateJWT,
    check('id', 'The ID provided is not valid').isMongoId(),
    check('id').custom( verifyIfIdExist ),
    validateFields
], deleteUser)

module.exports = router