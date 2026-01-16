import getLogger from '../utils/logger'
import formidable from 'formidable'
import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand, ObjectCannedACL, S3ServiceException } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import fs from 'fs'
import path from 'path'

const logger = getLogger('Upload')

// AWS S3 Client
const s3 = new S3Client({
    region: process.env.AWS_S3_REGION,
    credentials: {
      accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY!,
    },
})
const bucketName = process.env.AWS_S3_BUCKET_NAME!

async function doesS3ObjectExist(bucket: string, key: string): Promise<boolean> {
    try {
        await s3.send(new HeadObjectCommand({
            Bucket: bucket,
            Key: key,
        }))
        return true
    } catch (err: unknown) {
        if (err instanceof S3ServiceException && err.name === 'NotFound') return false
        throw err // other errors, like permission denied
    }
}

export async function uploadUserAvatar(file: formidable.File, userId: string) {
    try {
        const fileStream = fs.createReadStream(file.filepath)
        const fileExt = path.extname(file.originalFilename || '')
        const key = `user/${userId}/avatar${fileExt}` // S3 上的檔案路徑
        console.log('key:', key)
        const uploadParams = {
            Bucket: bucketName,
            Key: key,
            Body: fileStream,
            ContentType: file.mimetype || 'application/octet-stream',
            ACL: 'private' as ObjectCannedACL,
        }
        await s3.send(new PutObjectCommand(uploadParams))
        logger.info('上傳圖片成功')
    } catch (error) {
        logger.error('上傳圖片失敗', error)
        throw new Error('上傳圖片失敗')
    }
}

export async function getUserAvatarUrl(userId: string, filename: string): Promise<string | null> {
    try {
        const key = `user/${userId}/${filename}` // S3 上的檔案路徑
        const exists = await doesS3ObjectExist(bucketName, key)
        if (!exists) return null

        const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: key,
        })
        const url = await getSignedUrl(s3, command, { expiresIn: 60 * 60}) // 簽名 URL 有效期 1 小時
        logger.info(`pre-signed URL: ${url}`)
        return url
    } catch (error) {
        logger.error('取得圖片失敗', error)
        throw new Error('取得圖片失敗')
    }
}