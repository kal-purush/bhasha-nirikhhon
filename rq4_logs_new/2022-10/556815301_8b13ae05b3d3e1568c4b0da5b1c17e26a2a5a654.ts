import { Request, Response, NextFunction } from 'express'

import { AuthModel } from '../core/models'

const authModel = new AuthModel()

export const login = async (req: Request, res: Response) => {
  const { email, password } = req.body

  const userCredentials = await authModel.login({ email, password })

  if (!userCredentials) {
    res.status(409).json({ message: 'El usuario no existe' })
  }

  res.status(200).json({
    message: 'Usuario logeado correctamente',
    data: {
      ...userCredentials,
    },
  })
}

export const register = async (req: Request, res: Response) => {
  const { email, password, firstName, lastName, avatar } = req.body

  const userCredentials = await authModel.register({
    email,
    password,
    firstName,
    lastName,
    avatar,
  })

  if (!userCredentials) {
    res.status(409).json({ message: 'El usuario ya existe' })
  }

  res.status(201).json({
    message: 'Usuario registrado correctamente',
    data: { ...userCredentials },
  })
}

export const logout = async (req: Request, res: Response) => {
  const { ID } = req.params
  const { password } = req.body

  const deletedUser = await authModel.logout({ id: ID, password })

  if (!deletedUser) {
    res
      .status(409)
      .json({ message: 'El usuario no fue eliminado correctamente' })
  }

  res.status(201).json({
    message: deletedUser,
  })
}