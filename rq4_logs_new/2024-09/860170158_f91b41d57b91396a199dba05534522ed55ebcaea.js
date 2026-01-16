const express = require('express');
const bcryptjs = require('bcryptjs');
const jwt = require("jsonwebtoken");

const UsersModel = require('../models/users');

const router = express.Router()

router.post("/", async (req, res) => {
  // #swagger.tags = ['Autenticação']
  // #swagger.summary = 'Realizar login'

  try {
    const user = await UsersModel.findOne({ email: req.body.email });
    if (user) {
      
      const result = await bcryptjs.compare(req.body.password, user.password);
      if (result) {

        const token = await jwt.sign({ email: user.email }, process.env.TOKEN_SECRET);
        res.json({ token });

      } else {
        res.status(400).json({ error: "Usuário e/ou senha inválidos" });
      }

    } else {
      res.status(400).json({ error: "Usuário e/ou senha inválidos" });
    }
  } catch (error) {
    res.status(400).json({ error });
  }
});

router.post('/cadastrar', async (req, res) => {
  // #swagger.tags = ['Autenticação']
  // #swagger.summary = 'Realizar cadastro'

  try {
    const { email, password } = req.body;

    const userExists = await UsersModel.findOne({ email });
    if (userExists) {
      return res.status(422).send('Este nome de usuário não está disponível');
    }

    const salt = await bcryptjs.genSalt(10);  
    const hashPassword = await bcryptjs.hash(password, salt);

    const user = new UsersModel({
      email,
      password: hashPassword,
    });

    const newUser = await user.save();
    res.status(201).json(newUser);

  } catch (error) {
    res.status(400).json({ error: error });
  }
});

module.exports = router;