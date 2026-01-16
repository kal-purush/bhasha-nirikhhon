import jwt from 'jsonwebtoken'

import { Request, Response } from 'express'
import { JWT_SECRET } from '../index'

export const decodedToken = async (req: Request, res: Response) => {
  const token = req.headers.authorization?.split(' ')[1] // Extract the JWT token from the Authorization header

  if (!token) {
    return res.status(401).json({
      error: {
        code: 401,
        message: 'Unauthorized',
      },
    })
  }

  const decodedToken = jwt.verify(token, JWT_SECRET)

  return decodedToken
}