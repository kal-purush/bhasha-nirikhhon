const objectStore = require('ibm-cos-sdk');
const stream = require('stream');
const sharp = require('sharp');
const path = require('path');

const apiKeyId = process.env.APIKEY;
const endpoint = process.env.ENDPOINT;
const serviceInstanceId = process.env.SERVICE_INSTANCE;
const bucket = process.env.BUCKET_NAME;

const config = {
  endpoint,
  apiKeyId,
  serviceInstanceId,
};

const cos = new objectStore.S3(config);

function listObjects(thebucket) {
  return cos.listObjects({
    Bucket: thebucket,
  }).promise();
}

function uploadFromStream(thebucket, thekey) {
  const pass = new stream.PassThrough();
  const filename = path.parse(thekey).name;
  let extension = path.parse(thekey).ext;
  console.log("UPLOADING ", filename, extension, thebucket, thekey);
  if (extension === '') {
    extension = '.png';
  }
  const params = { Bucket: thebucket, Key: `${filename}-thumbnail${extension}`, Body: pass };
  cos.upload(params, (err) => {
    if (err) {
      console.log(err);
    }
  });
  return pass;
}

async function main() {
  const objects = await listObjects(bucket);
  for (let i = 0; i < objects.Contents.length; i += 1) {
    const key = objects.Contents[i].Key;
    console.log("KEY ", key);
    if (!key.includes('-thumbnail')) {
      const inStream = cos.getObject({ Bucket: bucket, Key: key }).createReadStream();
      console.log("IN STREAM MADE");
      const transform = sharp().resize({ width: 200, height: 200 });
      inStream.pipe(transform).pipe(uploadFromStream(bucket, key));
    }
  }
}

if (require.main === module) {
  main();
}