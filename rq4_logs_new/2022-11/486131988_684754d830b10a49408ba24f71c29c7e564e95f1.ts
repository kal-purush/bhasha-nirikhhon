//import { expect } from 'chai'

import {
	displayError,
	error_to_string,
} from '.'

import { createError } from '@offirmo/error-utils'


describe(`@offirmo-private/print-error-to-ansi`, () => {

	describe('displayError()', () => {

		it.only('should work -- trivial error', () => {
			const err = new Error('foo')
			;(err as any).statusCode = 555

			displayError(err)
			console.log()
			console.error(error_to_string(err))
		})

		it('should work -- typed error', () => {
			const err = createError('foo', {
				statusCode: 555,
				foo: 42,
				framesToPop: 3,
			})

			displayError(err)
			console.log()
			console.error(error_to_string(err))
		})

		it('should work -- advanced error', () => {
			const err = createError('foo', {
				statusCode: 555,
				foo: 42,
				framesToPop: 3,
			})

			displayError(err)
			console.log()
			console.error(error_to_string(err))
		})

		describe('cause chaining', function () {


			it('should work -- simple error', () => {
				const err = createError('foo', {
					statusCode: 555,
					foo: 42,
					framesToPop: 3,
				})

				displayError(err)
				console.log()
				console.error(error_to_string(err))
			})


			it('should work -- advanced error', () => {
				const err = createError('foo', {
					statusCode: 555,
					foo: 42,
					framesToPop: 3,
				})

				displayError(err)
				console.log()
				console.error(error_to_string(err))
			})


			it('should work -- infinite loop', () => {
				const err = createError('foo', {
					statusCode: 555,
					foo: 42,
					framesToPop: 3,
				})

				displayError(err)
				console.log()
				console.error(error_to_string(err))
			})
		})
	})
})