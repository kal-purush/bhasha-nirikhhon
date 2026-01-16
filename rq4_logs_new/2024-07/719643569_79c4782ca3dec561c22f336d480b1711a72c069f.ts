import { supabase } from "@/config/supabase"
import {v4 as uuidv4} from "uuid"

export const bucketRouter = {
    uploadFile: async (file: File) => {
        const fileExtension = file.name.split(".").pop()
        const fileName = `${uuidv4()}.${fileExtension}`
        let filePath = `public/${fileName}`;
        const { error } = await supabase.storage.from('product_image_storage').upload(filePath, file);
        if (error) {
            throw error;
        }

        const { data } = supabase.storage.from('product_image_storage').getPublicUrl(filePath);
        return data.publicUrl
    }
}