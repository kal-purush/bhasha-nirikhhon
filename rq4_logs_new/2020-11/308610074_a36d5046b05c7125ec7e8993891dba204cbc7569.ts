import { APIGatewayProxyHandler } from 'aws-lambda';
import * as AWS from 'aws-sdk';
const { PREFIX, REGION, BUCKET } = require('../constants.json');

export const importProductsFile: APIGatewayProxyHandler = async (event, _context) => {

    const s3 = new AWS.S3({ region: REGION });

    const { name } = event.queryStringParameters;
    try {
        if (!name.endsWith('csv')) throw Error('File extension does not fit requirements. It should be an [CSV] file');
        console.debug("FILENAME: [%s]", name);
        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*"
            },
            // body: JSON.stringify(`https://${BUCKET}.s3-${REGION}.amazonaws.com/${PREFIX}/${name}`),
            body: await s3.getSignedUrlPromise("putObject", { Bucket: BUCKET, Key: `${PREFIX}/${name}`, Expires: 60, ContentType: "text/csv" })
        };
    } catch (error) {
        console.log(error);
        return {
            statusCode: 406,
            headers: {
                "Access-Control-Allow-Origin": "*",
                Accept: 'text/csv'
            },
            body: error.message
        };
    }
};

