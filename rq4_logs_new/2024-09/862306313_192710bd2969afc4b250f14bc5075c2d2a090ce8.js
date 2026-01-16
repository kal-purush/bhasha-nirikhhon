if(process.env.NODE_ENV !="production"){
  require('dotenv').config();
}

const express = require("express");
const mongoose = require("mongoose");
const cors = require('cors');
const session = require('express-session');
const passport = require('passport');
const LocalStrategy = require("passport-local");
const User = require("./models/User");
const multer = require("multer");
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const File = require("./models/File");
const cloudinary = require('cloudinary').v2;
const B2 = require('backblaze-b2');
const axios = require('axios');
const { Redis } = require('@upstash/redis');
const Category=require("./models/Category");
const{Readable}=require('stream');

const app = express();

const upload = multer({ dest: 'uploads/' });

// Upstash Redis configuration
const redis = new Redis({
  url:process.env.REDIS_URL,
  token:process.env.REDIS_TOKEN,
});

app.use(cors({
  origin: 'http://localhost:5173',
  credentials: true
}));



app.use(express.static(path.join(__dirname, '../Front-End/dist'))); // Adjust path as necessary

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const sessionOptions = {
  secret:process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized:false,
  cookie: {
    expires: Date.now() + 7 * 24 * 60 * 60 * 1000,
    maxAge: 7 * 24 * 60 * 60 * 1000,
    httpOnly: true
  }
};

app.use(session(sessionOptions));
app.use(passport.initialize());
app.use(passport.session());
passport.use(new LocalStrategy(User.authenticate()));
passport.serializeUser(User.serializeUser());
passport.deserializeUser(User.deserializeUser());

const dburl =process.env.MONGODB_URL;

mongoose.connect(dburl, {})
  .then(() => {
    console.log('Connected to MongoDB Atlas');
  })
  .catch((err) => {
    console.error('Connection error', err);
  });

// CLOUDINARY
cloudinary.config({
  cloud_name:process.env.CLOUDINARY_CLOUD_NAME,
  api_key:process.env.CLOUDINARY_API_KEY,
  api_secret:process.env.CLOUDINARY_API_SECRET,
});

// BACKBLAZE B2
const b2 = new B2({
  applicationKeyId:process.env.BLACKBAZE_APPL_KEY_ID,
  applicationKey:process.env.BLACKBAZE_APPL_KEY
});
let b2BucketId =process.env.BLACKBAZE_BUCKET_ID;

// Function to split file into chunks
const splitFileIntoChunks = async(filePath, chunkSize) => {
  const fileBuffer = await fs.promises.readFile(filePath);
  const chunks = [];
  let currentIndex = 0;

  while (currentIndex < fileBuffer.length) {
    const chunk = fileBuffer.slice(currentIndex, currentIndex + chunkSize);
    chunks.push(chunk);
    currentIndex += chunkSize;
  }

  return chunks;
};


const uploadChunkToCloudinary = async (chunk) => {
  try {
    const stream = new Readable();
    stream.push(chunk);
    stream.push(null);

    const result = await new Promise((resolve, reject) => {
      stream.pipe(
        cloudinary.uploader.upload_stream({ resource_type: 'raw' }, (error, result) => {
          if (error) {
            return reject(error);
          }
          resolve(result);
        })
      );
    });
    console.log(result);
    return result.public_id; // Store this in your database
  } catch (error) {
    console.error('Error uploading to Cloudinary:', error); // Log the full error object
    throw new Error('Failed to upload chunk to Cloudinary');
  }
};


const uploadChunkToB2 = async (chunk, filename) => {
  try {
    await b2.authorize();
    const uploadUrlResponse = await b2.getUploadUrl({ bucketId: b2BucketId });
    const uploadUrl = uploadUrlResponse.data.uploadUrl;
    const authorizationToken = uploadUrlResponse.data.authorizationToken;

    const ext = path.extname(filename).toLowerCase();
    let contentType = 'application/octet-stream'; // Default

    switch (ext) {
            case '.mp4':
              contentType = 'video/mp4';
              break;
            case '.avi':
              contentType = 'video/x-msvideo';
              break;
            case '.mkv':
              contentType = 'video/x-matroska';
              break;
            case '.mov':
              contentType = 'video/quicktime';
              break;
          }

    const encodedFileName = encodeURIComponent(filename);

    const response = await axios.post(uploadUrl, chunk, {
      headers: {
        'Authorization': authorizationToken,
        'Content-Type': contentType,
        'X-Bz-File-Name': encodedFileName,
        'X-Bz-Content-Sha1': crypto.createHash('sha1').update(chunk).digest('hex')
      },
    });

    return response.data.fileId;
  } catch (error) {
    console.error('Error uploading to Backblaze B2:', error.response ? error.response.data : error.message);
    throw new Error('Failed to upload chunk to Backblaze B2');
  }
};




// Function to upload chunk to Upstash Redis
const uploadChunkToUpstashRedis = async (chunk, filename) => {
  try {
    const chunkKey = `chunk:${filename}`;
    await redis.set(chunkKey, chunk);
    return chunkKey; // Return the key for later retrieval
  } catch (error) {
    console.error('Error uploading to Upstash Redis:', error.message);
    throw new Error('Failed to upload chunk to Upstash Redis');
  }
};


// Function to distribute chunks to different services
let globalChunkIndex = 0;
const distributeChunks = (chunks) => {
  const services = [
    { name: 'Cloudinary', func: uploadChunkToCloudinary },
    { name: 'Backblaze B2', func: uploadChunkToB2 },
    { name: 'Upstash Redis', func: uploadChunkToUpstashRedis },
  ];

  return chunks.map((chunk) => {
    const service = services[globalChunkIndex % services.length];
    const filename = `chunk_${globalChunkIndex++}`; // Increment the global counter for each chunk
    return { func: service.func, chunk, filename, service: service.name };
  });
};


// ---------------------
app.post("/api/uploadFile", upload.single('fileUpload'), async (req, res) => {
  const filePath = req.file.path;
  const chunkSize = 200 * 1024;
  const chunks = await splitFileIntoChunks(filePath, chunkSize);
  console.log(chunks);

  const fileBuffer = fs.readFileSync(filePath);
  const checkSum = crypto.createHash('sha256').update(fileBuffer).digest('hex');
  const fileExtension = path.extname(req.file.originalname);

  const distributeChunk = distributeChunks(chunks);

  // Sequential upload of chunks
  try {
    const uploadedChunks = [];
    for (const { func, chunk, filename, service } of distributeChunk) {
      const uploadedPath = await func(chunk, filename);
      uploadedChunks.push({ service, path: uploadedPath });
    }

    const file = new File({
      name: req.file.originalname,
      chunks: uploadedChunks,
      chunkSize: chunkSize,
      originalSize: fs.statSync(filePath).size,
      checksum: checkSum,
      fileExtension: fileExtension
    });

    await file.save();

    const typeOfFile = req.body.fileType;
    await Category.updateOne({ name: typeOfFile }, { $push: { files: file._id } });

    await fs.promises.unlink(filePath);

    let cat = await Category.find({ name: typeOfFile }).populate('files');

    res.status(201).json({
      message: 'File uploaded and chunked successfully',
      file: cat
    });
  } catch (error) {
    console.error('Error during upload:', error.message);
    res.status(500).send('Error during file upload');
  }
});


app.post('/api/getCategory',async(req,res)=>{
  
  let data=await Category.find({}).populate("files");
  res.json({data});
});



// FILE RETRIEVAL
const retrieveChunkFromCloudinary = async (publicId) => {
  try {
    console.log(`Retrieving chunk from Cloudinary for public_id: ${publicId}`);

    // Retrieve resource metadata from Cloudinary
    const result = await cloudinary.api.resource(publicId, { resource_type: 'raw' });

    console.log('Cloudinary result:', result); // Debugging the result

    if (!result || !result.secure_url) {
      throw new Error('Chunk not found in Cloudinary');
    }

    // Fetch the actual file content from the secure URL
    const fileUrl = result.secure_url;
    const response = await axios.get(fileUrl, { responseType: 'arraybuffer' });

    return response.data;
  } catch (error) {
    // Enhanced error handling
    if (error.response) {
      console.error('Error retrieving from Cloudinary:', error.response.data);
    } else {
      console.error('Error retrieving from Cloudinary:', error.message);
    }
    throw new Error('Failed to retrieve chunk from Cloudinary');
  }
};



const retrieveChunkFromB2 = async (fileId) => {
  try {
    await b2.authorize(); // Ensure authorization
    const response = await b2.downloadFileById({ fileId });
    return Buffer.from( response.data); // File content
  } catch (error) {
    console.error('Error retrieving from Backblaze B2:', error.response ? error.response.data : error.message);
    throw new Error('Failed to retrieve chunk from Backblaze B2');
  }
};

const retrieveChunkFromUpstashRedis = async (filename) => {
  try {
    const chunkKey = `${filename}`;
    const chunk = await redis.get(chunkKey);
    return Buffer.from(chunk); // Convert chunk to Buffer for reassembly
  } catch (error) {
    console.error('Error retrieving from Upstash Redis:', error.message);
    throw new Error('Failed to retrieve chunk from Upstash Redis');
  }
};


// ---------------------------------------------
const reassembleChunks = async (file) => {
  const chunkPromises = file.chunks.map(async ({ service, path }) => {
    if (service === 'Cloudinary') {
      return await retrieveChunkFromCloudinary(path);
    } else if (service === 'Backblaze B2') {
      return await retrieveChunkFromB2(path);
    } else if (service === 'Upstash Redis') {
      return await retrieveChunkFromUpstashRedis(path);
    }
  });

  // Wait for all chunk promises to resolve
  const reassembledFile = await Promise.all(chunkPromises);
  return Buffer.concat(reassembledFile);
};




app.get('/api/retrieveFile/:fileId', async (req, res) => {
  try {
    const fileId = req.params.fileId;
    const file = await File.findById(fileId);

    if (!file) {
      return res.status(404).send('File not found');
    }

    const reassembledFile = await reassembleChunks(file);

    // Set the appropriate content type based on the file extension
    let contentType = 'application/octet-stream'; // Default
    switch (file.fileExtension) {
      case '.pdf':
        contentType = 'application/pdf';
        break;
      case '.jpg':
      case '.jpeg':
        contentType = 'image/jpeg';
        break;
      case '.png':
        contentType = 'image/png';
        break;
      case '.mp4':
        contentType = 'video/mp4';
        break;
      // Add other cases as needed
    }

    res.set({
      'Content-Type': contentType,
      // 'Content-Disposition': `attachment; filename="${file.name}${file.fileExtension}"`, // Include file extension
      'Content-Disposition': `attachment; filename="${file.name}"`, 
      'Content-Length': reassembledFile.length
    });

    res.send(reassembledFile);
  } catch (error) {
    console.error('Error retrieving file:', error.message);
    res.status(500).send('Failed to retrieve file');
  }
});



app.post('/api/signup', async (req, res) => {
  try {
      let { username, email, password } = req.body;
      const newUser = new User({ email, username });
      const registeredUser = await User.register(newUser, password);
      console.log(registeredUser);

      // Using req.login as a Promise
      req.login(registeredUser, (err) => {
          if (err) {
              console.log('Error during login:', err);
              return res.status(500).json({
                  message: 'Login failed',
                  error: err.message
              });
          }
          console.log("You are  logged In after signup");
          
          // Send response here after successful login
          return res.status(201).json({
              message: 'Successful',
              user: {
                  id: registeredUser._id,
                  username: registeredUser.username,
                  email: registeredUser.email
              }
          });
      });

  } catch (error) {
      console.log('Error during registration:', error);

      return res.status(501).json({
          message: 'Failed',
          error: error.message
      });
  }
});



app.post('/api/login', (req, res, next) => {
  passport.authenticate('local', (err, user, info) => {
      if (err) {
          return res.status(500).json({ message: 'Internal server error' });
      }
      if (!user) {
          return res.status(401).json({ message: info.message || 'Login failed' });
      }
      req.logIn(user, (err) => {
          if (err) {
              return res.status(500).json({ message: 'Internal server error' });
          }
          return res.json({ message: 'Login successful', user });
      });
  })(req, res, next);
});



app.post('/api/isLoggedIn', (req, res) => {
  if (req.isAuthenticated()) {
    console.log("You are logged In");
    res.json({
      loggedIn: true,
      user: req.user 
    });
  } else {
    res.json({
      loggedIn: false,
      user:null,
      message: "You are not logged in",
    });
  }
});


app.post('/api/logout', (req, res) => {
  req.logout((err) => {
    if (err) {
      return res.status(500).json({ message: 'Logout failed', error: err });
    }

    return res.status(200).json({ message: 'Logout successful' });
  });
});



app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../Front-End/dist', 'index.html')); // Adjust path as necessary
});


app.listen(8080, () => {
  console.log("App is listening");
});