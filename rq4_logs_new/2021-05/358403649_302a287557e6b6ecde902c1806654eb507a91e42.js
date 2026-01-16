const express = require('express')
const Atendimentos = require('../models/atendimentosModel')

const router = express.Router()

router.get('/', async (req, res) => {
    try {
        const atendimentos = await Atendimentos.find()
        res.json(atendimentos)
    } catch (err) {
        return res.status(400).send({ error: 'Ocorreu falha ao listar atendimentos' })
    }
})

router.post('/', async (req, res) => {
    try {
        const atendimento = await Atendimentos.create(req.body)
        return res.send([atendimento])
    } catch (err) {
        return res.status(400).send({ error: 'Ocorreu falha ao criar atendimento\n' + err })
    }
})

module.exports = app => app.use('/atendimentos', router)