var AWS = require('aws-sdk');
var fs = require('fs')
     //aws credentials
     AWS.config = new AWS.Config();
     AWS.config.accessKeyId = "AKIAIDUUQC2BCV4QCV5A";
     AWS.config.secretAccessKey = "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";
     AWS.config.region = "ap-southeast-1";
     AWS.config.apiVersions = {
        "s3": "2006-03-01"
     }

    var s3 = new AWS.S3({signatureVersion: 'v4'});

    var bodystream = fs.createReadStream("pic.jpg");

    var params = {
        'Bucket': 'prakhargyan',
        'Key': 'uploads/images/' + "test.jpg",
        'Body': bodystream,
        'ContentEncoding': 'base64', 
        'ContentType ': 'image/jpeg'
     };

     //also tried with s3.putObject
     s3.upload(params, function(err, data){
        console.log('after s3 upload====', err, data);
     }) 