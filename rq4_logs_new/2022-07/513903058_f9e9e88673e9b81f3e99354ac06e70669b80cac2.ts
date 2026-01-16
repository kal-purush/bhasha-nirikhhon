import { Request } from "@pepperi-addons/debug-server";
import { ResourceField, ResourceFields, RESOURCE_TYPES } from "./constants";
import PapiService from "./papi.service";

export class Core
{
	protected resource: string;

	constructor(protected request: Request, protected papi: PapiService)
	{
		this.resource = this.request.query.resource_name;
	}

	/**
     * returns the schema of the requested resource
     */
	public async createSchema(): Promise<any>
	{
		this.validateCreateSchemaPrerequisites();
		const resourceFields: ResourceFields = await this.papi.getResourceFields(this.resource);
		const schema = this.translateResourceFieldsToSchema(resourceFields);

		return schema;
	}

	protected validateCreateSchemaPrerequisites()
	{
		if(!RESOURCE_TYPES.includes(this.resource))
		{
			const errorMessage = `The resource name is not valid. Please provide a valid resource name.`;
			console.error(errorMessage);
			throw new Error(errorMessage);
		}
	}

	/**
     * @param resourceFields The resource's fields to be translated to a valid Resource Schema
     * @returns a valid Resource Schema
     */
	protected translateResourceFieldsToSchema(resourceFields: ResourceFields)
	{
		const schema = {
			Name: this.resource,
			Type: 'papi',
			GenericResource: true,
			Fields: {}
		};

		const uniqueFields = ['WrntyID', 'UUID', 'ExternalID'];

		for (const resourceField of resourceFields)
		{
			schema.Fields[resourceField.Label] = {
				Type: this.getFieldTypeFromFieldsFormat(resourceField),
				Unique: uniqueFields.includes(resourceField.Label),
			}
		}
        
		return schema;
	}


	/**
     * @param resourceField The resource field from which a field type will be extracted
     * @returns Returns a valid schema field type based on a ResourceField Format
     */
	protected getFieldTypeFromFieldsFormat(resourceField: ResourceField)
	{
		switch(resourceField.Format) 
		{
		case 'String':
		case 'Guid':
			return 'String'
		case 'DateTime':
			return 'DateTime'
		case 'Int32':
		case 'Int64':
			return 'Integer'
		case 'Boolean':
			return 'Bool'
		case 'Double':
		case 'Decimal':
			return 'Double'
		default:
			return 'String'
		}
	}
}