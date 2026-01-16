import { ExtractJwt } from 'passport-jwt'

import { IPassportConfig } from '../src'

export default {
	passportJwtWrapper: {
		i18next: {
			preload: ['en', 'sk'],
			fallbackLng: 'en',
			ns: ['error', 'translation'],
			defaultNS: 'translation',
			detection: {
				order: ['header']
			},
			backend: {
				loadPath: 'locales/{{lng}}/{{ns}}.json',
				jsonIndent: 2
			},
			nsSeparator: ':',
			keySeparator: false
		},
		passport: <IPassportConfig>{
			local: {
				usernameField: 'email',
				passwordField: 'password',
				session: false,
				passReqToCallback: true
			},
			jwt: {
				secretOrKey: process.env.JWT_SECRET,
				api: {
					exp: '15m',
					jwtFromRequest: ExtractJwt.fromExtractors([ExtractJwt.fromAuthHeaderAsBearerToken(), ExtractJwt.fromUrlQueryParameter('t')]),
					refresh: {
						exp: '4h'
					}
				},
				passwordReset: {
					exp: '4h',
					jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken()
				},
				invitation: {
					jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
					exp: '30d'
				}
			}
		}
	}
}