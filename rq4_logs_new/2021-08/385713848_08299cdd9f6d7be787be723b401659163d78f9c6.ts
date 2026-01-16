import cloudinary from 'cloudinary'
import Env from '@ioc:Adonis/Core/Env'
import Application from '@ioc:Adonis/Core/Application'

export class ImageUploader {
  constructor() {
    cloudinary.v2.config({
      cloud_name: Env.get('CLOUDINARY_NAME'),
      api_key: Env.get('CLOUDINARY_API_KEY'),
      api_secret: Env.get('CLOUDINARY_API_SECRET'),
      secure: true,
    })
  }

  public async upload(file: string, callback?: cloudinary.UploadResponseCallback) {
    //
    // VERIFICAR DEMORA NO (CREATE) NA HORA DO UPLOAD
    //

    const folder = Application.inDev
      ? 'solasol_catalog_dev'
      : Application.inProduction
      ? 'solasol_catalog_prod'
      : 'solasol_catalog_test'

    const res = await cloudinary.v2.uploader.upload(
      file,
      {
        folder: folder,
      },
      callback
    )

    return res
  }

  public async destroy(publicId: string) {
    const res = await cloudinary.v2.uploader.destroy(publicId)

    return res
  }
}