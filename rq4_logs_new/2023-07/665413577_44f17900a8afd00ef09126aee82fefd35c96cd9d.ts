import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';

import { DataService } from './dataService';
import type { queryResultType } from './dataService';

// https://docs.aws.amazon.com/lambda/latest/dg/typescript-handler.html
export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    const dataService = new DataService();
    const { results } = (await dataService.singleQuery(
      'SELECT "Hello World";'
    )) as queryResultType;
    return {
      statusCode: 200,
      body: JSON.stringify({
        results,
      }),
    };
  } catch (err: any) {
    console.error(err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: err,
      }),
    };
  }
};