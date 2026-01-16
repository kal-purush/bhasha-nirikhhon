import { HttpException, HttpStatus } from '@nestjs/common'

import { User } from '../user.entity'
import { LoginDto } from '../dto/login.dto'
import { RegisterDto } from '../dto/register.dto'

export const mockUser: User = {
    id: 1,
    email: 'm@m.com',
    password: 'hash'
}

export const mockLoginDto: LoginDto = {
    email: 'm@m.com',
    password: 'mmm'
}

export const mockRegisterDto: RegisterDto = {
    email: 'm@m.com',
    password: 'mmm'
}

export const invalidCredentialsException = new HttpException(
    'Invalid credentials',
    HttpStatus.CONFLICT
)

export const emailIsTakenException = new HttpException(
    'Email is already taken',
    HttpStatus.CONFLICT
)