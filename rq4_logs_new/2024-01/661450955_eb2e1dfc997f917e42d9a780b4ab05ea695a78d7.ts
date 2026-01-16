import { createClient } from '@supabase/supabase-js'
import { CallError } from '../helpers/callError'
import * as fastifyMultipart from '@fastify/multipart'

const supabase = createClient(
  process.env.SUPABASE_URL as string,
  process.env.SUPABASE_KEY as string,
)

export async function uploadFile(upload: fastifyMultipart.MultipartFile) {
  const time = new Date().getTime().toString()

  const fileName = time.concat(upload.filename).split(' ').join('_')

  const { data, error } = await supabase.storage
    .from('file')
    .upload(fileName, upload.file, {
      duplex: 'half',
      upsert: true,
      contentType: upload.mimetype,
      cacheControl: 'max-age=31536000',
    })
  if (error) {
    return new CallError(error.message, 400)
  } else {
    return data
  }
}