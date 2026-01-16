import { SchemaRegistry } from '@sprucelabs/schema'
import { SpruceErrors } from '../errors.types'



const mercuryResponseErrorSchema: SpruceErrors.MercuryTypes.MercuryResponseErrorSchema  = {
	id: 'mercuryResponseError',
	namespace: 'MercuryTypes',
	name: 'Mercury response error',
	    fields: {
	            /** . */
	            'responseErrors': {
	                type: 'raw',
	                isRequired: true,
	                isArray: true,
	                options: {valueType: `AbstractSpruceError<any>`,}
	            },
	    }
}

SchemaRegistry.getInstance().trackSchema(mercuryResponseErrorSchema)

export default mercuryResponseErrorSchema