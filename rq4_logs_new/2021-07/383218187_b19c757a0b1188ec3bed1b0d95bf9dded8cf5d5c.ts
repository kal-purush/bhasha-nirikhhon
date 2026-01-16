import express, { Request, Response, NextFunction, Application } from "express";
const User = require("../models/Users")
const jwt= require("jsonwebtoken")
const { SECRET_TOKEN} = process.env;


export default function verifyToken (req, res, next){
    const token = req.headers["x-access-token"]
  if(!token){
    return res.status(401).json({auth: false, message: "No token provided"})
  }
  const decoded = jwt.verify(token,SECRET_TOKEN )
  req.userId = decoded.id
  console.log(decoded)
  next()
}