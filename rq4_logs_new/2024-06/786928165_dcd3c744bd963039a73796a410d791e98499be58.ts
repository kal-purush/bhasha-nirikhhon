import { getErrorMessage } from "@/utils/get-error-message";
import swell from "@/lib/server";

export const deleteSwellS3File = async (id: string) => {
  if (!id) return getErrorMessage("No id provided to deleteSwellS3File");

  try {
    const res = await swell.delete("/content/s-3-files/{id}", {
      id,
    });

    return res;
  } catch (error) {
    console.log("error trying to delete the image in Swell: ", error);
    return getErrorMessage(error);
  }
};