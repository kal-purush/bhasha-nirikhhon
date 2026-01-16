import { Path, POST, FormParam } from 'typescript-rest'
import { Inject } from 'typescript-ioc'
const bCrypt = require('bcrypt-nodejs')
const jwt = require('jsonwebtoken')
import JWTPassport from '../security/passport'
import Database from '../services/database'
import User from '../models/user.model'

@Path('/api')
export class RegisterController {
  @Inject
  private database: Database

  @Path('/register')
  @POST
  async register(user: any): Promise<string> {
    const salt = bCrypt.genSaltSync()
    const passwordHash = await bCrypt.hashSync(user.password, salt, undefined)

    user.salt = salt
    user.password = passwordHash

    return this.database.models.User.create(user)
  }
}