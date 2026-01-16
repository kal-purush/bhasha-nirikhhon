import AbstractSpruceTest, { test, assert } from '@sprucelabs/test'
import eventContractUtil from '../../utilities/eventContract.utility'

export default class EventContractUtilityTest extends AbstractSpruceTest {
	@test()
	protected static async canParseEventName() {
		const nameParts = eventContractUtil.splitEventNameWithOptionalNamespace(
			'test.event'
		)
		assert.isEqualDeep(nameParts, {
			eventName: 'event',
			eventNamespace: 'test',
		})
	}

	@test()
	protected static async canJoinEventName() {
		const name = eventContractUtil.joinEventNameWithOptionalNamespace({
			eventNamespace: 'test',
			eventName: 'event',
		})
		assert.isEqual(name, 'test.event')
	}
}