import { getRepository } from 'typeorm';
import { User } from '../entities/user'
import {APP_SECRET} from '../consts'

var jwt = require('jsonwebtoken');

export default {

  Mutation: {

    createUser: async (_, { data }, context ) => {

      const token = context.request.headers.authorization

      try {

        jwt.verify(token, APP_SECRET);

        const user: User = await getRepository(User).findOne({email: data.email});

        if (!user) {
          throw new Error ("Email ja existe")
          return false
        }

        const verifyPassword = (password: string) => {
          var valido = true;

          const digitMatch = password.match(/\d/);
          const hasDigit = digitMatch && digitMatch.length > 0;

          if (!hasDigit) {
            throw new Error("Falta Digito")
            valido = false;
          }

          const letterMatch = password.match(/\D/);
          const hasLetter = letterMatch && letterMatch.length > 0;

          if (!hasLetter) {
            throw new Error("Falta letra maiuscula")
            valido = false;
          }

          const hasMinimumSize = password.length >= 7;
          if (!hasMinimumSize) {
            throw new Error("Falta letra minuscula")
            valido = false;
          }
          return valido
        }
        const pass = verifyPassword(data.password)

        if (!pass) {
          throw new Error ("Senha invalida")
          return false
        }

        return user 
      } catch {
        throw new Error ("Nao autorizado")
      }
    }
  }
}