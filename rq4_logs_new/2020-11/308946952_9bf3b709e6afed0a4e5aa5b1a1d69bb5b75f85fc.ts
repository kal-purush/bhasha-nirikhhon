import { S3Handler } from 'aws-lambda';
import AWS from 'aws-sdk';
import 'source-map-support/register';
import csv from 'csv-parser';

const BUCKET = 'lshutau-rs-uploaded';

export const importFileParser: S3Handler = async (event, _context) => {
  try {
    const s3 = new AWS.S3({ region: 'eu-west-1' });

    for (const record of event.Records) {
      const s3Stream = s3.getObject({
        Bucket: BUCKET,
        Key: record.s3.object.key,
      }).createReadStream();

      await new Promise((resolve, reject) => {
        s3Stream.pipe(csv())
          .on('data', data => {
            console.log(data);
          })
          .on('end', async () => {
            try {
              await s3.copyObject({
                Bucket: BUCKET,
                CopySource: `${BUCKET}/${record.s3.object.key}`,
                Key: record.s3.object.key.replace('uploaded', 'parsed'),
              }).promise();
    
              await s3.deleteObject({
                Bucket: BUCKET,
                Key: record.s3.object.key,
              }).promise();
              resolve()
            } catch (err) {
              reject(err);
            }
          });
      });
    }
  } catch (err) {
    console.log(err);
    throw err;
  }
}