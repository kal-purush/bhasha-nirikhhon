const user = require("../model/users");
const db = require("../database_Connection/config");
const Joi = require('joi');
const jwt = require('jsonwebtoken');

const schemaSignin = Joi.object({
  userName: Joi.string(),
  password: Joi.string().required(),
});
//this function to get all data for user from data base
exports.getAllUsers = async (req, res, next) => {
  try {
    const [users, _] = await user.findAll();
    res.status(200).send({ users });
  }
  catch (error) {
    next(error);
  }
};

//this function to get all data for user from data base
exports.createNewUsers = async (req, res, next) => {
  try {
    let name = req.body.name;
    let userName = req.body.userName;
    let password = req.body.password;
    let myQuery = `INSERT INTO users (userName,password,name)
    VALUES('${userName}','${password}','${name}')`;
    db.execute(myQuery).then(() =>
      res.status(200).json({ success: true, message: "users Added !" })
    );
  }
  catch (error) {
    next(error);
  }
};

//this function to compared if userName and password is correct and give token
exports.signin = async (req, res) => {
  const admins = await user.findUser(req.body.userName, req.body.password)
  //validate data entry to new user
  const { error } = schemaSignin.validate(req.body)
  if (error) return res.status(400).send({ status: 400, message: error.details[0].message })
  //Check if email is exsist
  if (admins[0].length == 0) return res.status(400).send({ status: 400, message: 'the userName or password is wrong' });
  // Create a token and assign it to the user
  const token = jwt.sign({ id: admins[0].id }, process.env.TOKEN_SECRET)
  res.header('auth-token', token).status(200).send({ status: 200, message: 'success', Token: token })
}