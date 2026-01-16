import { validateSchemaValues } from '@sprucelabs/schema'
import permissionContractSchema from '#spruce/schemas/mercuryTypes/v2020_09_01/permissionContract.schema'
import { PermissionContract } from '../mercury.types'

export default function buildPermissionContract<C extends PermissionContract>(
	contract: C
): C {
	validateSchemaValues(permissionContractSchema, contract)
	return contract as C
}