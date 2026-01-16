import { SupabaseClient } from '@supabase/supabase-js'
import { CallError } from '../../helpers/callError'
import * as fastifyMultipart from '@fastify/multipart'

export class UploadFile {
  constructor(private supabase: SupabaseClient) {}

  async image(upload: fastifyMultipart.MultipartFile) {
    const time = new Date().getTime().toString()

    const fileName = time.concat(upload.filename).split(' ').join('_')

    const { data, error } = await this.supabase.storage
      .from('file')
      .upload(fileName, upload.file, {
        duplex: 'half',
        upsert: true,
        contentType: upload.mimetype,
        cacheControl: 'max-age=31536000',
      })

    if (error) {
      throw new CallError(error.message, 400)
    } else {
      return data
    }
  }
}