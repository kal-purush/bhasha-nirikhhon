let db = require("../models");
let passwordHash = require("password-hash");
let jwt = require('jsonwebtoken');
let secretKey = 'secret';

let getAllUser = (req, res) => {
  if (req.decoded.role == "admin") {
    db.User
      .findAll()
      .then((data) => {
        res.send(data)
      })
      .catch((err) => {
        res.send(err.message)
      })
  }else {
    res.send("Unauthorized!")
  }

}

let getSingleUser = (req, res) => {
  if (req.decoded.role == "admin" || req.decoded.role == "guest") {
    db.User
      .findAll(
        {where: {id: req.params.id}}
      )
      .then((data) => {
        res.send(data)
      })
      .catch((err) => {
        res.send(err.message)
      })
  }else {
    res.send("Unauthorized!")
  }
}

let createUser = (req, res) => {
  if (req.decoded.role == "admin") {
    db.User
      .create(
        {nama: req.body.nama, email: req.body.email}
      )
      .then((data) => {
        res.send(data)
      })
      .catch((err) => {
        res.send(err.message)
      })
  }else {
    res.send("Unauthorized!")
  }
}

let deleteUser = (req, res) => {
  if (req.decoded.role == "admin") {
    db.User
      .destroy(
        {where: {id: req.params.id}}
      )
      .then(() => {
        res.send(true)
      })
      .catch((err) => {
        res.send(err.message)
      })
  }else {
    res.send("Unauthorized!")
  }
}

let updateUser = (req, res) => {
  if (req.decoded.role == "admin" || req.decoded.role == "guest") {
    db.User
      .update(
        {nama: req.body.nama, email: req.body.email},
        {where: {id: req.params.id}}
      )
      .then((data) => {
        res.send(data)
      })
      .catch((err) => {
        res.send(err.message)
      })
  }else {
    res.send("Unauthorized!")
  }
}

let signupUser = (req, res) => {
  db.User
    .create(
      {nama: req.body.nama, email: req.body.email, password: passwordHash.generate(req.body.password), role: req.body.role}
    )
    .then((data) => {
      res.send(data)
    })
    .catch((err) => {
      res.send(err.message)
    })
}

let signinUser = (req, res) => {
  db.User
    .findOne(
      {where: {email: req.body.email}}
    )
    .then((data) => {
      let pass = passwordHash.verify(req.body.password, data.password);
      if (pass) {
        let token = jwt.sign({ email: data.email, role: data.role }, secretKey);
        res.send(token)
      }else {
        res.send("Email or password was wrong!")
      }
    })
    .catch((err) => {
      res.send(err.message)
    })
}

module.exports = {
  getAllUser, getSingleUser, createUser, deleteUser, updateUser, signupUser, signinUser
}