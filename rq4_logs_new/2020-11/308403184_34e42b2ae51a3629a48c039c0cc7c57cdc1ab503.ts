import { APIGatewayProxyHandler } from "aws-lambda";
import * as AWS from "aws-sdk";
import { withCorsHeaders } from "../../../utils/withCorsHeaders";

const { REGION, BUCKET } = process.env;

export const importProductsFile: APIGatewayProxyHandler = async (event) => {
  const fileName = event?.queryStringParameters?.name;
  const params = {
    Bucket: BUCKET,
    Key: `uploaded/${fileName}`,
    Expires: 60,
    ContentType: "text/csv",
  };

  if (!fileName) {
    return withCorsHeaders({
      statusCode: 400,
      body: JSON.stringify({
        message: "Invalid file name",
      }),
    });
  }

  try {
    const s3 = new AWS.S3({ region: REGION });
    const url = await s3.getSignedUrlPromise("putObject", params);

    return withCorsHeaders({
      statusCode: 200,
      body: url,
    });
  } catch (err) {
    return withCorsHeaders({
      statusCode: 500,
      body: JSON.stringify({
        message:
          err?.message || "Something went wrong. Please try again later.",
      }),
    });
  }
};