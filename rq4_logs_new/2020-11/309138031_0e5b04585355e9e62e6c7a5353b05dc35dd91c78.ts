import { S3Handler } from 'aws-lambda';
import { S3 } from 'aws-sdk';
import * as csv from 'csv-parser';

const BUCKET_NAME = process.env.BUCKET_NAME;

export const importFileParser: S3Handler = async (event) => {
  console.log('Lambda invocation with event: ', event);

  const s3 = new S3({ region: process.env.AWS_REGION });

  event.Records.forEach(record => {
    const source = record.s3.object.key;

    const fileStream = s3.getObject({
      Bucket: BUCKET_NAME,
      Key: source
    }).createReadStream();

    console.log(`Processed data from ${source}:`);

    fileStream.pipe(csv())
      .on('data', data => {
        console.log();
        console.log(data);
      })
      .on('error', error => {
        throw new Error(error.message);
      })
      .on('end', async () => {
        console.log('Data end');

        await s3.copyObject({
          Bucket: BUCKET_NAME,
          CopySource: `${BUCKET_NAME}/${source}`,
          Key: source.replace('uploaded', 'parsed')
        }).promise();


        await s3.deleteObject({
          Bucket: BUCKET_NAME,
          Key: source
        }).promise();

        console.log(`Object was moved to 'parsed' folder`);
      });
  });
};