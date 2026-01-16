import {response} from "express";

const express = require('express')
const router = express.Router()

const Category = require('../models/categoryModel')

router.get('/', async (req:any, res:any) => {
  const category = await Category.find({})
  res.status(200).json(category)
})

router.delete('/:id', async (req:any, res:any) => {
  const { id } = req.params
  const category = await Category.findOneAndDelete({ _id: id })
  res.status(200).json(category)
})

router.post('/', (req:any, res:any) => {
  const { name, description, color, icon } = req.body
  try {
    const category = Category.create({ name, description, color, icon })
    res.status(200).json(category)
  } catch (error: any) {
    res.status(400).json({error: error.message})
  }
})

module.exports = router