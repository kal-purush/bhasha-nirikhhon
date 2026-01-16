/* Dependencies */
import { Router, Request, Response } from 'express'
import * as jwt from 'jsonwebtoken'
import * as bcrypt from 'bcrypt'

/* Models */
import { User } from '../../models'

class AuthenticationHandler {
  router: Router

  constructor() {
    this.router = Router()
    this.init()
  }

  init() {
    this.router.post('/', this.authenticate)
  }

  authenticate(req: Request, res: Response) {
    const UserData = req.body

    if (UserData && UserData.email && UserData.password) {
      const { email, password } = UserData

      User.findOne({ where: { email } })
          .then(data => {

            if (!data) return res.status(400).json({ message: 'User not found' })

            const encryptedPassword = data.password

            bcrypt.compare(password, encryptedPassword)
                  .then(valid => {
                    const { dataValues } = data

                    if (valid) {
                      let token = jwt.sign({ email: dataValues.email, isAdmin: dataValues.isAdmin }, 'supersecret')
                      res.json({ token }) 
                    }
                    res.json({ message: 'invalid password'} )
                  })
          })
          .catch(err => {
            console.log(err)
          })
    } else {
      // Not enough info provided
      res.status(400).json({ message: 'must provide a username and password' })
    }
  }
}

export default new AuthenticationHandler().router