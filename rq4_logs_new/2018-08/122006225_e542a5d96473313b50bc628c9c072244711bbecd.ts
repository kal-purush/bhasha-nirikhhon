const functions = require('firebase-functions');
const mkdirp = require('mkdirp-promise');
const admin = require('firebase-admin');
const spawn = require('child-process-promise').spawn;
const path = require('path');
const os = require('os');
const fs = require('fs');

try {
    admin.initializeApp(functions.config().firebase);
} catch (e) {
}

// Max height and width of the thumbnail in pixels.
const THUMB_MAX_HEIGHT = 300;
const THUMB_MAX_WIDTH = 300;
const THUMB_PREFIX = 'thumb_';

export default class CreateThumbnailClass {

    public static createThumbnailFunction: any = functions.storage.object().onFinalize(async (object) => { // File and directory paths.
        const filePath = object.name;
        const contentType = object.contentType; // This is the image MIME type
        const fileDir = path.dirname(filePath);
        const fileName = path.basename(filePath);
        const thumbFilePath = path.normalize(path.join(fileDir, `${THUMB_PREFIX}${fileName}`));
        const tempLocalFile = path.join(os.tmpdir(), filePath);
        const tempLocalDir = path.dirname(tempLocalFile);
        const tempLocalThumbFile = path.join(os.tmpdir(), thumbFilePath);

        console.log('FilePath = ', filePath);
        console.log('contentType = ', contentType);
        console.log('fileDir = ', fileDir);
        console.log('tempLocalThumbFile = ', tempLocalThumbFile);

        // Exit if this is triggered on a file that is not an image.
        if (contentType.startsWith('image/')) {
            // Exit if the image is already a thumbnail.
            if (!fileName.startsWith(THUMB_PREFIX)) {
                // Cloud Storage files.
                const bucket = admin.storage().bucket(object.bucket);
                const file = bucket.file(filePath);
                const thumbFile = bucket.file(thumbFilePath);
                const metadata = {
                    contentType: contentType,
                    // To enable Client-side caching you can set the Cache-Control headers here. Uncomment below.
                    // 'Cache-Control': 'public,max-age=3600',
                };

                // Create the temp directory where the storage file will be downloaded.
                await mkdirp(tempLocalDir);
                // Download file from bucket.
                await file.download({destination: tempLocalFile});
                console.log('The file has been downloaded to', tempLocalFile);
                // Generate a thumbnail using ImageMagick.
                await spawn('convert', [tempLocalFile, '-thumbnail', `${THUMB_MAX_WIDTH}x${THUMB_MAX_HEIGHT}>`, tempLocalThumbFile], {capture: ['stdout', 'stderr']});
                console.log('Thumbnail created at', tempLocalThumbFile);
                // Uploading the Thumbnail.
                await bucket.upload(tempLocalThumbFile, {destination: thumbFilePath, metadata: metadata});
                console.log('Thumbnail uploaded to Storage at', thumbFilePath);
                // Once the image has been uploaded delete the local files to free up disk space.
                fs.unlinkSync(tempLocalFile);
                fs.unlinkSync(tempLocalThumbFile);
                // Get the Signed URLs for the thumbnail and original image.
                const config = {
                    action: 'read',
                    expires: '03-01-2500',
                };
                const results = await Promise.all([
                    thumbFile.getSignedUrl(config),
                    file.getSignedUrl(config),
                ]);
                console.log('Got Signed URLs.');
                const thumbResult = results[0];
                const originalResult = results[1];
                const thumbFileUrl = thumbResult[0];
                const fileUrl = originalResult[0];
                // Add the URLs to the Database
                await admin.database().ref('images').push({path: fileUrl, thumbnail: thumbFileUrl});
                console.log('Thumbnail URLs saved to database.');
            } else {
                console.error('Préfix thumbnail trouvé dans le nom de l\'image');
            }
        } else {
            console.error('Ce n\'est pas une image');
        }
    });
}