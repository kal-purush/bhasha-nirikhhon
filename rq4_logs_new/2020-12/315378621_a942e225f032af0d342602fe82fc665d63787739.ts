import AbstractSpruceError from '@sprucelabs/error'
import { errorAssertUtil } from '@sprucelabs/test-utils'

const mercuryErrorAssertUtil = {
	assertError(
		error: Error | AbstractSpruceError<any>,
		expectedCode: string,
		expectedPartialOptions?: Record<string, any> | undefined
	) {
		errorAssertUtil.assertError(error, 'MERCURY_RESPONSE_ERROR')
		errorAssertUtil.assertError(
			(error as any).options.responseErrors[0],
			expectedCode,
			expectedPartialOptions
		)
	},
}

export default mercuryErrorAssertUtil