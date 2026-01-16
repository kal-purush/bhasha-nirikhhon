import { UserConflictError } from '../util/errors'
import { JsonController, Body, Post, BadRequestError, Authorized, Get, CurrentUser } from 'routing-controllers'

import { getToken, registerUser, userExists } from './auth.service'
import { User } from '../entities/User'
import { encodeToken } from '../util/jwt'

@JsonController('/auth')
export default class {
    @Post('/token')
    async login(@Body() { email, password }) {
        const token = await getToken({ email, password })
        if (!token) throw new BadRequestError()

        return { token }
    }

    @Post('/register')
    async register(@Body() user: User) {
        const { email } = user

        const exists = await userExists(email)
        if (exists) throw new UserConflictError()

        const created = await registerUser(user)
        if (!created) throw new BadRequestError()

        return { token: await encodeToken({ email }) }
    }

    @Authorized()
    @Get('/verify')
    async verify(
        @CurrentUser({ required: true })
        user: User
    ) {
        return user
    }
}