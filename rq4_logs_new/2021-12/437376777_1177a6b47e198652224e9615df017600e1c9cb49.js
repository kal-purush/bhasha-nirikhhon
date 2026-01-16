/* eslint-disable no-undef */
const { processFileForUpload } = require('../lib/util');
const { uploadFile } = require('../services/AWS/s3');
const allowedImageFormats = process.env.ALLOWED_IMAGE_FORMATS.split(',');
const allowedVideoFormats = process.env.ALLOWED_VIDEO_FORMATS.split(',');

module.exports.uploadFile = (req, res) => {
  const filePath = req.body.file.path;
  const fileExtension = (
    filePath.substring(filePath.lastIndexOf('.') + 1) + ''
  ).toLowerCase();
  const uploadOptions = {
    bucketName: req.body.bucketName || null,
    fileName: req.body.fileName || null,
    fileType: fileExtension,
  };
  if (
    allowedImageFormats.includes(fileExtension) ||
    allowedVideoFormats.includes(fileExtension)
  ) {
    const params = processFileForUpload(req.body.file, uploadOptions);
    uploadFile(params)
      .then((location) => {
        res.send(location);
      })
      .catch((error) => {
        res.send(error);
      });
  } else {
    res.send('File Format not supported');
  }
};