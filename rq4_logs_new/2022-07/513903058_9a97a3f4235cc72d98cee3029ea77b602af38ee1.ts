import { Client, Request } from '@pepperi-addons/debug-server';
import { CoreService } from './core.service';
import { Helper } from './helper';
import PapiService from './papi.service';

export async function get_by_key(client: Client, request: Request) 
{
	console.log(`Query received: ${JSON.stringify(request.query)}`);

	switch (request.method) 
	{
	case "GET": {
		const papiClient = Helper.getPapiClient(client);
		const papiService = new PapiService(papiClient);
		const core = new CoreService(request.query?.resource_name, request, papiService);

		return await core.getByKey();
	}
	default: {
		throw new Error(`Unsupported method: ${request.method}`);
	}
	}
}