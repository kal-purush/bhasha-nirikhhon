import {response} from "express";

const express = require('express')
const router = express.Router()

const Expense = require('../models/expenseModel')

router.get('/', async (req:any, res:any) => {
  const expense = await Expense.find({})
  res.status(200).json(expense)
})

router.delete('/:id', async (req:any, res:any) => {
  const { id } = req.params
  const expense = await Expense.findOneAndDelete({ _id: id })
  res.status(200).json(expense)
})

router.post('/', (req:any, res:any) => {
  const { name, description, value, categoryId, tagIds } = req.body
  try {
    const expense = Expense.create({ name, description, value, categoryId, tagIds })
    res.status(200).json(expense)
  } catch (error: any) {
    res.status(400).json({error: error.message})
  }
})

module.exports = router