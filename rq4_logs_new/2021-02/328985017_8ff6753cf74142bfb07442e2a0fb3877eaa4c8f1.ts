import * as AWS  from 'aws-sdk'
import { createLogger } from '../utils/logger'
import {PresignedUrl} from '../models/PresignedUrl'
const bucketName = process.env.TODO_S3_BUCKET;
const urlExpiration = process.env.SIGNED_URL_EXPIRATION;

const logger = createLogger('business-presignedUrls')

export async function generateUploadUrl(bucketKey:string): Promise<PresignedUrl>{
    
  const s3 = new AWS.S3({
        signatureVersion: 'v4'
      })
    
      const settings = { 
        Bucket: bucketName, 
        Key: bucketKey, 
        Expires: urlExpiration 
      }
    
      logger.info('PresignedUrl requested for',{settings});
    
      const uploadUrl = s3.getSignedUrl('putObject', settings)
    
      logger.info('PresignedUrl created',{
        uploadUrl
      });
  
      // build retrieval url
      const retrievalUrl = `https://${bucketName}.s3.amazonaws.com/${bucketKey}`
      return {
        uploadUrl,
        retrievalUrl
      };
}