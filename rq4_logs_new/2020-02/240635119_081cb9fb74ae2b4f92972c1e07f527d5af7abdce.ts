import 'source-map-support/register'

import { APIGatewayProxyEvent, APIGatewayProxyHandler, APIGatewayProxyResult } from 'aws-lambda'
// import { UpdateBookRequest } from '../../requests/UpdateBookRequest'
import { createLogger } from '../../utils/logger'
import { getBook } from '../../business-logic/books'
const logger = createLogger('getBook')


export const handler: APIGatewayProxyHandler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  const bookId = event.pathParameters.bookId
  logger.info("Processing the get request for bookId :", bookId)
  
  const result = await getBook(bookId)

  logger.info("Update todo request was successful", bookId)
  return {
    statusCode: 201,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true
    },
    body: JSON.stringify(result)
  }
}