import { PapiBatchResponse, ResourceFields } from './constants';

export interface IPapiService 
{

	getResourceFields(resourceName: string): Promise<ResourceFields>;

	createResource(resourceName: string, body: any);

	updateResource(resourceName: string, body: any);

	upsertResource(resourceName: string, body: any);

	batch(resourceName: string, body: any): Promise<PapiBatchResponse>;

	getResources(resourceName: string, query: any);

	getResourceByKey(resourceName: string, key: string): Promise<any> ;

	getResourceByExternalId(resourceName: string, externalId: any);

	getResourceByInternalId(resourceName: string, internalId: any);

	searchResource(resourceName: string, body: void);
}

export default IPapiService;