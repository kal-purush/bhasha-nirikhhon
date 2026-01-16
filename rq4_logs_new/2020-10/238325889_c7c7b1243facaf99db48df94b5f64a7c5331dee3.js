const functions = require('firebase-functions');
const {Storage} = require('@google-cloud/storage');
const express = require("express");
const ffmpegPath = require("@ffmpeg-installer/ffmpeg").path
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs');
const path = require('path');
const Busboy = require('busboy');
const os = require('os');

ffmpeg.setFfmpegPath(ffmpegPath);


const main = express();
const app = express.Router();
main.use('/api',app);

const storage = new Storage({keyFile: "./My First Project-f84e2151b52b.json"});
const bucketName = "gs://phonic-altar-238300.appspot.com"
const tmpdir = os.tmpdir();

const multer = function(req, res, next) {
  const busboy = new Busboy({
    headers: req.headers,
    limits: {
      fileSize: 10 * 1024 * 1024,
    }
  });

  const fields = {};
  const files = [];
  const fileWrites = [];
  // Note: os.tmpdir() points to an in-memory file system on GCF
  // Thus, any files in it must fit in the instance's memory.

  busboy.on('field', (key, value) => {
    // You could do additional deserialization logic here, values will just be
    // strings
      console.log(key + " " + value);
    fields[key] = value;
  });

  busboy.on('file', (fieldname, file, filename, encoding, mimetype) => {
    const filepath = path.join(tmpdir, filename);
      console.log("filename");
    console.log(`Handling file upload field ${fieldname}: ${filename} (${filepath})`);
    const writeStream = fs.createWriteStream(filepath);
    file.pipe(writeStream);

    fileWrites.push(new Promise((resolve, reject) => {
      file.on('end', () => writeStream.end());
      writeStream.on('finish', () => {
          console.log(`${filename} `);
          files.push({
            fieldname,
            originalname: filename,
            encoding,
            mimetype,
              path: filepath
          })

          resolve();
      });
      writeStream.on('error', reject);
    }));
  });

  busboy.on('finish', () => {
    Promise.all(fileWrites)
      .then((ans) => {
        req.body = fields;
        req.files = files;
        return next();
      })
      .catch(next);
  });

  busboy.end(req.rawBody);
}

/*
const MulterConfig = multer.diskStorage({
    filename: function(req,file,cb) {
        cb(null,file.originalname);
    }
})
*/

//const upload = multer({storage: MulterConfig});

const convertFile =async (req,res,next) => {
    req.file = req.files[0];
    console.log(req.file);
    if(req.file.mimetype !== 'video/mp4'){
        var filename = req.file.originalname.split('.').slice(0, -1).join('.') + '.mp4';
        const Last_Path = req.file.path;
        req.file.path = path.join(tmpdir,filename);
        var command = ffmpeg(Last_Path)
            .format('mp4')
            .on('error',(err) =>{
                if(err){
                    return res.status(500).json({"message":"error "+ err})
                }
            })
            .on('end', (stdout, stderr) =>{
                console.log("finished");
                req.file.filename = filename;
                fs.unlink(Last_Path,(err) =>{
                    if(err){
                        return res.status(500).json({"error":"Can't delete the file"});
                    }
                    return next();
                })
            })
            .saveToFile(req.file.path)
    }
    else {
        next();
        return;
    }
}


const uploadFile = async(req,res) => {
    console.log(req.file);
    storage.bucket(bucketName).upload(req.file.path,{gzip: true})
    .then(ans =>{
        fs.unlink(req.file.path,(err) =>{
            if(err){
                return res.status(500).send("Cant delete the file");
            }
            return res.status(200).json({"message": "The file have been succesfully uploaded"})
        })
        return ;
    })
    .catch(err => res.status(500).json(err))
}

app.post('/video',multer,convertFile,uploadFile);

exports.main = functions.https.onRequest(main);