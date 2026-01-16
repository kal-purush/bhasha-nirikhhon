import { Request, Response } from 'express'

import { getAll, getByID, create } from '../models/user'

export const get = async (req: Request, res: Response) => {
  const users = await getAll()

  res.status(200).json(users)
}

export const getOne = async (req: Request, res: Response) => {
  const { ID } = req.params
  const user = await getByID(ID)

  res.status(200).json(user)
}

export const post = async (req: Request, res: Response) => {
  const { email, password } = req.body

  const user = await create({ email, password })

  res.status(201).json(user)
}