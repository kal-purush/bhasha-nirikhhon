import * as AWS from 'aws-sdk';
import { BUCKET, REGION } from '../constants/constants';
import csv from 'csv-parser';

export const importFileParser = (event, _context) => {
    const s3 = new AWS.S3({ region: REGION });
    console.log('RECORDS', event.Records);

    event.Records.forEach(record => {
        const s3Stream = s3.getObject({
            Bucket: BUCKET,
            Key: record.s3.object.key
        }).createReadStream();

        s3Stream.pipe(csv())
            .on('data', data => {
                console.log('[importFileParser]: data:', data);
            })
            .on('error', (err) => {
                console.error(err);
            })
            .on('end', async () => {
                console.log(`Copy from ${BUCKET}/${record.s3.object.key}`);

                await s3.copyObject({
                    Bucket: BUCKET,
                    CopySource: `${BUCKET}/${record.s3.object.key}`,
                    Key: record.s3.object.key.replace('uploaded', 'parsed')
                }).promise();

                await s3.deleteObject({
                    Bucket: BUCKET,
                    Key: record.s3.object.key
                }).promise();

                console.log(`Copied into ${BUCKET}/${record.s3.object.key.replace('uploaded', 'parsed')}`);
            });
    });

}