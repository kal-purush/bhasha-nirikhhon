import * as jwtoken from "jsonwebtoken"
import { Request, Response, NextFunction } from "express"

export function auth(req: Request, res: Response, next: NextFunction) {
   const authHeader = req.header("Authorization")
   const token = authHeader && authHeader.split(" ")[1]

   if (!token) {
      return res.status(400).send({
         status: "Failed",
         message: "Unauthorized"
      })
   }

   try {
      const secret = process.env.SECRET_KEY || "secret_key"
      const verified = jwtoken.verify(token, secret)
      req.body = verified
      next()
   } catch (err: any) {
      console.log(err)
      res.status(400).send({
			status: 'failed',
			message: 'invalid token'
		})
   }
}