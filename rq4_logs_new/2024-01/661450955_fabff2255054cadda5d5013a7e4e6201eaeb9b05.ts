import { supabase } from '../../external/supabase/createClient'
import { UploadImageController } from './uploadController'
import { UploadService } from './uploadService'

const UploadFactory = () => {
  const uploadService = new UploadService(supabase)
  const uploadController = new UploadImageController(uploadService)
  return uploadController
}

export const uploadFactory = UploadFactory()