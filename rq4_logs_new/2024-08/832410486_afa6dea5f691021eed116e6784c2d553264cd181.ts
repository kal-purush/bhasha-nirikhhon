import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';
import fs from 'fs';

const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION,
});

export const uploadFileToS3 = async (file: any): Promise<string> => {
    const fileContent = fs.readFileSync(file.filepath);
    const fileExtension = file.originalFilename?.split('.').pop();
    const fileName = `${uuidv4()}.${fileExtension}`;

    const params = {
        Bucket: process.env.AWS_S3_BUCKET_NAME!,
        Key: fileName,
        Body: fileContent,
        ContentType: file.mimetype!,
    };

    const data = await s3.upload(params).promise();
    return data.Location; // S3에 업로드된 파일의 URL
};