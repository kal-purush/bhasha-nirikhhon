import { deleteObject } from "firebase/storage";
import { getImagesRef } from "./client"
import { FileNameUtil } from "./util";

export const deleteImage = async (fileName: string, fileNameUtil: FileNameUtil) => {
  const uniqueFileName = fileNameUtil.generateUniqueFileName(fileName);
  const fileRef = getImagesRef(uniqueFileName);
  try {
    await deleteObject(fileRef);
  } catch (e: any) {
    let errorMessage: string;
    switch (e.code) {
      case 'storage/object-not-found': {
        errorMessage = 'Object not found';
        break;
      }
      case 'storage/unauthenticated':
      case 'storage/unauthorized': {
        errorMessage = 'Access denied';
        break;
      }
      default: {
        errorMessage = 'Unknown error occurred';
        break;
      }
    }
    throw Error(`Failed to delete image: ${errorMessage}`);
  }
}