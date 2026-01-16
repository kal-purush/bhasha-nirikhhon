import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';
import { S3 } from 'aws-sdk';
import { BUCKET_NAME, REGION } from '../config';

export const invoke: APIGatewayProxyHandler = async (event, _context) => {
  const fileName = event.queryStringParameters?.name;
  if (!fileName) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        error: 'no query string name with file name!'
      })
    }
  }

  const s3 = new S3({ region: REGION});

  const params = {
    Bucket: BUCKET_NAME,
    Key: `uploaded/${fileName}`,
    Expires: 60,
    ContentType: 'text/csv',
  };

  try {
    const url = await s3.getSignedUrlPromise("putObject", params);
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify(url)
    }
  } catch (err) {
    console.error("Error while creating signed url: ", err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Internal Server Error: cannot create signed url"
      })
    }
  }
}