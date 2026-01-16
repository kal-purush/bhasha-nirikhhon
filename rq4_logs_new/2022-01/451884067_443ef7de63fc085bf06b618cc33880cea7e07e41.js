const User = require("../model/user");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");


const register = async (req, res) => {
    let role
    if (req.params.role === "owner") {
     role = "owner";
    }else if(req.params.role === "client") {
        role = "client";   
    }

  const salt = await bcrypt.genSalt(12);
  const hashedPassword = await bcrypt.hash(req.body.password, salt);
  const user = await User.create({
    name: req.body.name,
    email: req.body.email,
    password: hashedPassword,
    role : role,
  })
    res.send(user)
};

const login = async (req, res) => {
  const user = await User.findOne({ email: req.body.email });
  if (!user) return res.status(404).send("Email doesn't exist");

  const validPassword = await bcrypt.compare(req.body.password, user.password);
  if (!validPassword) return res.status(404).send("password wrong");

  const token = jwt.sign({
    _id: user.id,
    name: user.firstName,
    role: user.role,
  },process.env.SECRET);

  res.header("auth-token", token).send(token);
};

module.exports =  {
    register, login
};