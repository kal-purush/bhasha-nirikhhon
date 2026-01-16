const aws = require('aws-sdk');

class S3Handle {
    public S3Client: any;
    constructor() {
        aws.config.update({
            endpoint: process.env.S3_ENDPOINT,
            accessKeyId: process.env.S3_ACCESS_KEY,
            secretAccessKey: process.env.S3_SECRET_KEY,
            region: process.env.S3_REGION,
            s3BucketEndpoint: true,
            sslEnabled: false,
            s3ForcePathStyle: true, // needed with minio?
            signatureVersion: 'v4'
        });
        this.S3Client = new aws.S3();
    }
}

export default new S3Handle();