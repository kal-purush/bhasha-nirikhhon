import { JwtService } from '@nestjs/jwt'
import { UnauthorizedError } from './erros/unauthorized'
import { User } from 'src/user/schema/user.schema'
import { StatusCode } from '../enum/statusCode'
import { ConfigService } from '@nestjs/config'
import jwt from 'jsonwebtoken'


export class Token {

    private static readonly jwtService: JwtService
    private static readonly configService: ConfigService


    public static generateToken(user: User): string {
        const playload = {
            email: user.email,
            username: user.username,
        }
        const secretKey = this.configService.get<string | undefined>('SECRET_KEY')
        console.log(secretKey)
        return this.jwtService.sign(playload, {
            secret: secretKey,
            expiresIn: '60min'
        })
    }

    // public static verifyToken(token: string): jwt.JwtPayload {
    //     try {
    //         return this.jwtService.verify(token, this.SECRET_KEY) as jwt.JwtPayload
    //     } catch {
    //         throw new UnauthorizedError('invalid token', StatusCode.UNAUTHORIZED)
    //     }
    // }
}