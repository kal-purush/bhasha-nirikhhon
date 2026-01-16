import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
	DynamoDBDocumentClient,
	PutCommand,
	PutCommandInput,
} from '@aws-sdk/lib-dynamodb';
import type {
	APIGatewayProxyEvent,
	APIGatewayProxyResult,
	Handler,
} from 'aws-lambda';
import { randomUUID } from 'crypto';

// Interfaces
interface RequestPayload {
	name: string;
	message: string;
}

interface ResponseBody {
	message: string;
	id?: string;
}

// Initialize DynamoDB Document Client
const client = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(client);

// Helper function to create API Gateway responses
const createResponse = (
	statusCode: number,
	body: ResponseBody | { error: string },
): APIGatewayProxyResult => ({
	statusCode,
	headers: {
		'Content-Type': 'application/json',
		'Access-Control-Allow-Origin': '*', // Adjust CORS policy as needed
		'Access-Control-Allow-Credentials': true,
	},
	body: JSON.stringify(body),
});

// Lambda Handler
export const handler: Handler<
	APIGatewayProxyEvent,
	APIGatewayProxyResult
> = async (event): Promise<APIGatewayProxyResult> => {
	console.log('Received event:', JSON.stringify(event, null, 2));

	// 1. Get DynamoDB table name from environment variables
	const tableName = process.env.DYNAMODB_TABLE_NAME;
	if (!tableName) {
		console.error('Error: DYNAMODB_TABLE_NAME environment variable not set.');
		return createResponse(500, {
			error: 'Internal server error: Configuration missing.',
		});
	}
	console.log(`Using table: ${tableName}`);

	// 2. Parse and validate the request body
	let payload: RequestPayload;
	try {
		if (!event.body) {
			console.warn('Request body is missing.');
			return createResponse(400, { error: 'Request body is required.' });
		}
		payload = JSON.parse(event.body);
		console.log('Parsed payload:', payload);

		// Basic validation
		if (
			!payload.name ||
			typeof payload.name !== 'string' ||
			payload.name.trim() === ''
		) {
			return createResponse(400, {
				error: 'Missing or invalid field: name (must be a non-empty string).',
			});
		}
		if (
			!payload.message ||
			typeof payload.message !== 'string' ||
			payload.message.trim() === ''
		) {
			return createResponse(400, {
				error: 'Missing or invalid field: message (must be a non-empty string).',
			});
		}
	} catch (error) {
		console.error('Error parsing JSON body:', error);
		return createResponse(400, { error: 'Invalid JSON format in request body.' });
	}

	// 3. Prepare the item for DynamoDB
	const itemId = randomUUID();
	const createdAt = new Date().toISOString();
	const itemToInsert = {
		id: itemId,
		name: payload.name.trim(),
		message: payload.message.trim(),
		createdAt: createdAt,
	};

	const params: PutCommandInput = {
		TableName: tableName,
		Item: itemToInsert,
	};

	// 4. Write the item to DynamoDB
	try {
		console.log(`Attempting to write item to DynamoDB: ${JSON.stringify(params)}`);
		await ddbDocClient.send(new PutCommand(params));
		console.log(`Successfully wrote item ${itemId} to table ${tableName}.`);

		// 5. Return success response
		return createResponse(201, {
			message: 'Record successfully created.',
			id: itemId,
		});
	} catch (error) {
		console.error(`Error writing to DynamoDB table ${tableName}:`, error);
		// Check if error is an instance of Error to access message safely
		const errorMessage =
			error instanceof Error ? error.message : 'Unknown error';
		return createResponse(500, {
			error: `Internal server error: Could not save record. ${errorMessage}`,
		});
	}
}; 