import * as AWS from "aws-sdk";
import * as csv from "csv-parser";
import { withCorsHeaders } from "../../../utils/withCorsHeaders";

const { REGION, BUCKET } = process.env;

export const importFileParser = async function (event) {
  try {
    const s3 = new AWS.S3({ region: REGION });

    await Promise.all(
      event.Records.map(async (record) => {
        const source = record.s3.object.key;

        await logRecord(s3, source);
        await moveToParsedFolder(s3, source);
      })
    );

    return withCorsHeaders({
      statusCode: 202,
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

const logRecord = (s3: AWS.S3, source: string) =>
  new Promise((resolve, reject) => {
    s3.getObject({
      Bucket: BUCKET,
      Key: source,
    })
      .createReadStream()
      .pipe(csv())
      .on("data", (data) => {
        console.log("Data:", data);
      })
      .on("end", resolve)
      .on("error", reject);
  });

const moveToParsedFolder = async (s3: AWS.S3, source: string) => {
  await s3
    .copyObject({
      Bucket: BUCKET,
      CopySource: `${BUCKET}/${source}`,
      Key: source.replace("uploaded", "parsed"),
    })
    .promise();

  await s3
    .deleteObject({
      Bucket: BUCKET,
      Key: source,
    })
    .promise();
};