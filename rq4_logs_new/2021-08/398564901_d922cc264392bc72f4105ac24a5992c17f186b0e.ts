import { BaseHttpResponse } from "App/Controllers/Base/CrudController";
import { APIGatewayEvent } from "aws-lambda";

export default abstract class Logger {
    public static outgoingResponse(event: APIGatewayEvent, context, response: BaseHttpResponse | null) {
        const { body, headers, httpMethod, path, requestContext: { identity: { sourceIp } } } = event
        const { awsRequestId } = context
        const request = {
            body,
            headers,
            sourceIp,
            httpMethod,
            path,
            awsRequestId
        }

        if (response) {
            process.env.ENV !== 'local' && console.log(JSON.stringify({
                level: 'INFO',
                request,
                response
            }))
        }
    }

    public static ERROR(message, status, error) {
        console.log(JSON.stringify({
            level: 'ERROR',
            message,
            status,
            error: error.message
        }))
    }
}