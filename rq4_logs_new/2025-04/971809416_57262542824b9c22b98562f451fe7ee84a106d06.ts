import { Handler } from 'aws-lambda';

export const handler: Handler = async (event: Record<string, any>): Promise<Record<string, any>> => {

		return {
			statusCode: 200,
			body: JSON.stringify({
				message: 'Hello from Lambda!',
				input: event,
			}),
		};

};