import {
  getStorage,
  ref,
  getDownloadURL,
  uploadBytesResumable,
  deleteObject,
} from 'firebase/storage';
import { initializeApp } from 'firebase/app';
import APIError from './APIError';
import { fileLogger } from '../loggers/app-logger';
import { Request } from 'express';

const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY || 'string';
const FIREBASE_AIUTH_DOMAIN = process.env.FIREBASE_AIUTH_DOMAIN || 'string';
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || 'string';
const FIREBASE_STORAGE_BUCKET = process.env.FIREBASE_STORAGE_BUCKET || 'string';
const FIREBASE_MESSAGING_SENDER_ID = process.env.FIREBASE_MESSAGING_SENDER_ID || 'string';
const FIREBASE_APP_ID = process.env.FIREBASE_APP_ID || 'string';

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: FIREBASE_API_KEY,
  authDomain: FIREBASE_AIUTH_DOMAIN,
  projectId: FIREBASE_PROJECT_ID,
  storageBucket: FIREBASE_STORAGE_BUCKET,
  messagingSenderId: FIREBASE_MESSAGING_SENDER_ID,
  appId: FIREBASE_APP_ID,
};

const uploadToFireBase = async(req: Request, folder?: string):Promise <string | null> => {
  initializeApp(firebaseConfig);
  if (!req.file){
    throw new APIError('An image should be uploaded using \
      multer before using this utility', 400);
  }
  const storage = getStorage();
  // add date in ms with the name to prevent overriding images with the same name
  const storageRef = ref(storage, `files/${folder}/${Date.now()}-${req.file.originalname}`);
  const metadata = {
    contentType: req.file.mimetype,
  };
  const snapshot = await uploadBytesResumable(storageRef, req.file.buffer, metadata);
  const downloadURL = await getDownloadURL(snapshot.ref);
  fileLogger.info(`Image uploaded successfully to firebase, URL:${downloadURL}`);
  return downloadURL;
};

const deleteFromFirebase = async(fileUrl: string): Promise<void> => {
  initializeApp(firebaseConfig);

  const storage = getStorage();

  // Extract the file path from the URL
  const decodedUrl = decodeURIComponent(fileUrl);
  console.log(decodedUrl);
  const baseUrl = `https://firebasestorage.googleapis.com/v0/b/${firebaseConfig.storageBucket}/o/`;
  const filePath = decodedUrl.split(baseUrl)[1]?.split('?')[0];

  if (!filePath) {
    throw new Error('Invalid file URL');
  }

  // Create a reference to the file to delete
  const fileRef = ref(storage, filePath);

  // Delete the file
  await deleteObject(fileRef);
  fileLogger.info(`Image deleted successfully from firebase, URL: ${fileUrl}`);
};

export { uploadToFireBase, deleteFromFirebase };