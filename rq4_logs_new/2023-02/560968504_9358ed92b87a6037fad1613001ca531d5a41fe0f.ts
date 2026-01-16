import { Express } from 'express'
import request, { Response } from 'supertest'
import { expect } from 'chai'

import { ApiAuth, Logout, RefreshToken } from '../../../src'

import LoginRouter from '../../mocks/loginRouter'
import schemaMiddleware from '../../mocks/middlewares/schemaMiddleware'
import TestingEndpoint from '../../mocks/testingEndpoint'
import errorMiddleware from '../../mocks/middlewares/errorMiddleware'

import * as skTranslations from '../../../locales/sk/translation.json'
import * as enTranslations from '../../../locales/en/translation.json'

export function setupRouters(app: Express) {
	const loginRouter = LoginRouter()

	loginRouter.post('/logout', ApiAuth.guard(), schemaMiddleware(Logout.requestSchema), Logout.endpoint)
	loginRouter.post('/refresh-token', schemaMiddleware(RefreshToken.requestSchema), RefreshToken.endpoint)

	app.use('/auth', loginRouter)

	app.get('/endpoint', ApiAuth.guard(), TestingEndpoint)

	app.use(errorMiddleware)
}

export async function runNegativeRefreshTokenAttempt(app: Express, refreshToken: string): Promise<Response> {
	const response = await request(app).post('/auth/refresh-token').send({
		refreshToken
	})

	expect(response.statusCode).to.eq(401)

	return response
}

export function getLogoutMessage(language?: string): string {
	if (language && language === 'sk') {
		return skTranslations['You were successfully logged out']
	}

	return enTranslations['You were successfully logged out']
}