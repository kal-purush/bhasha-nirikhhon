import AbstractSpruceTest, { test, assert } from '@sprucelabs/test'
import SpruceError from '../../errors/SpruceError'
import eventResponseUtil from '../../utilities/eventResponse.utility'
import mercuryErrorAssertUtil from '../../utilities/mercuryErrorAssert.utility'

export default class MercuryErrorUtilTest extends AbstractSpruceTest {
	private static readonly errorWithoutErrorInstance = {
		totalContracts: 0,
		totalErrors: 0,
		totalResponses: 0,
		responses: [
			{
				errors: [
					{
						options: { code: 'COOL_ERROR', foo: 'bar' },
					},
				],
			},
		],
	}

	private static readonly twoErrorsWithoutErrorInstance = {
		totalContracts: 0,
		totalErrors: 0,
		totalResponses: 0,
		responses: [
			{
				errors: [
					{
						options: { code: 'COOL_ERROR', foo: 'bar' },
					},
					{
						options: { code: 'ANOTHER_ERROR', foo: 'bar' },
					},
				],
			},
		],
	}

	@test()
	protected static canAssertMercuryError() {
		const err = assert.doesThrow(() =>
			eventResponseUtil.getFirstResponseOrThrow(
				eventResponseUtil.mutatingMapAggregateResultsErrorsToSpruceErrors(
					this.errorWithoutErrorInstance,
					SpruceError
				)
			)
		)

		assert.doesThrow(() =>
			mercuryErrorAssertUtil.assertError(err, 'COOL_ERROR', {
				foo: 'bar2',
			})
		)

		mercuryErrorAssertUtil.assertError(err, 'COOL_ERROR', {
			foo: 'bar',
		})
	}

	@test()
	protected static canAssertMercuryErrorFromRawResponse() {
		const response = eventResponseUtil.mutatingMapAggregateResultsErrorsToSpruceErrors(
			this.errorWithoutErrorInstance,
			SpruceError
		)

		assert.doesThrow(() =>
			mercuryErrorAssertUtil.assertErrorFromResponse(response, 'COOL_ERROR', {
				foo: 'bar2',
			})
		)

		mercuryErrorAssertUtil.assertErrorFromResponse(response, 'COOL_ERROR', {
			foo: 'bar',
		})
	}

	@test()
	protected static throwsWithTooManyErrors() {
		const response = eventResponseUtil.mutatingMapAggregateResultsErrorsToSpruceErrors(
			this.twoErrorsWithoutErrorInstance,
			SpruceError
		)

		const err = assert.doesThrow(() =>
			mercuryErrorAssertUtil.assertErrorFromResponse(response, 'COOL_ERROR', {
				foo: 'bar',
			})
		)

		assert.doesInclude(err.message, 'more than 1')
	}
}