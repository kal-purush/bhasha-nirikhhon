import db from '../database/connection'
import { Request, Response } from 'express'
import bcrypt from 'bcrypt'
import { generateJwt, generateRefreshJwt } from '../helpers/jwt'

interface Account {
  id: number
  email: string
  username: string
  password: string
}

class SignIn {
  async index(req: Request, res: Response) {
    const { email, password } = req.body

    const Account: Account = await db('users').where('email', email).first()

    const match = Account
      ? bcrypt.compareSync(password, Account.password)
      : null

    if (!match) {
      return res.status(400).json({ message: 'Invalid email or password.' })
    }

    const token = generateJwt({ id: Account.id })
    const refreshToken = generateRefreshJwt({
      id: Account.id,
    })

    delete Account.password

    return res.status(200).json({
      ...Account,
      token,
      refreshToken,
      message: 'Successful login.',
    })
  }
}

export default SignIn