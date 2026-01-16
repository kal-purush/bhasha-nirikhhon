import { Request, Response } from 'express'

import {
  getAllUsers,
  getUserByID,
  createOneUser,
  updateOneUser,
  deleteOneUser,
} from '../core/models/user.model'

export const getUsers = async (req: Request, res: Response) => {
  const users = await getAllUsers()

  res.status(200).json(users)
}

export const getOneUser = async (req: Request, res: Response) => {
  const { ID } = req.params
  const user = await getUserByID(ID)

  res.status(200).json(user)
}

export const createUser = async (req: Request, res: Response) => {
  const { email, password } = req.body

  const user = await createOneUser({ email, password })

  res.status(201).json(user)
}

export const updateUser = async (req: Request, res: Response) => {
  const { ID } = req.params
  const { email } = req.body

  const user = await updateOneUser(ID, { email })

  res.status(200).json(user)
}

export const deleteUser = async (req: Request, res: Response) => {
  const { ID } = req.params
  const { password } = req.body

  const user = await deleteOneUser(ID, password)

  res.status(200).json(user)
}