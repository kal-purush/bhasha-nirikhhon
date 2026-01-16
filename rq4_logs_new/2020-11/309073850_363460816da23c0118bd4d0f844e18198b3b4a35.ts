import { S3Event, S3Handler } from 'aws-lambda';
import 'source-map-support/register';
import { S3 } from 'aws-sdk';
import { BUCKET_NAME, PARSED_FOLDER_NAME, REGION, UPLOAD_FOLDER_NAME } from '../config';
import * as csv from 'csv-parser';

//? if will be async word it will not work (related with stream)
export const invoke = (event: S3Event) => {
  const s3 = new S3({ region: REGION });

  for (const record of event.Records) {
    const initialKey = record.s3.object.key;

    const s3Stream = s3.getObject({
      Bucket: BUCKET_NAME,
      Key: initialKey
    }).createReadStream();

    s3Stream
      .pipe(csv())
      .on('data', (data) => {
        console.log('s3Stream data', data);
      })
      .on('error', (error) => {
        console.error("Parse csv error", error);
      })
      .on('end', async () => {
        const newKey = initialKey.replace(
          UPLOAD_FOLDER_NAME,
          PARSED_FOLDER_NAME
        );

        await s3.copyObject({
          Bucket: BUCKET_NAME,
          CopySource: `${BUCKET_NAME}/${initialKey}`,
          Key: newKey
        }).promise();

        await s3.deleteObject({
          Bucket: BUCKET_NAME,
          Key: initialKey
        }).promise();

        console.log(`Moved from ${BUCKET_NAME}/${initialKey} to ${BUCKET_NAME}/${newKey}`);
      });
  }

  return { statusCode: 202 }
}