import { Request, Response } from 'express'
import { Category } from '../models/categoryModel'

export const getAllCategories = async (req:Request, res:Response) => {
  try {
    const category = await Category.find({})
    res.status(200).json(category)
  } catch (error: any) {
    res.status(500).json({ error: error.message })
  }
}

export const createCategory = async (req:Request, res:Response) => {
  const { name, description, color, icon } = req.body
  try {
    const category = await Category.create({ name, description, color, icon })
    res.status(201).json(category)
  } catch (error: any) {
    res.status(400).json({error: error.message})
  }
}

export const deleteCategoryById = async (req:Request, res:Response) => {
  const { id } = req.params
  try {
    const category = await Category.findByIdAndDelete(id)
    if (!category) {
      res.status(404).json({ error: 'Category not found' })
      return
    }
    res.status(200).json(category)
  } catch (error: any) {
    res.status(500).json({ error: error.message })
  }
}