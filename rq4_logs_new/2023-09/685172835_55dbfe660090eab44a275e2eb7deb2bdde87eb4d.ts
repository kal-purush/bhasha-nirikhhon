import axios, { AxiosResponse } from "axios";

const imageUrlToBase64 = async (imageUrl: string): Promise<string | null> => {
  try {
    const response: AxiosResponse<ArrayBuffer> = await axios.get(imageUrl, {
      responseType: "arraybuffer",
    });
    const base64Image: string = Buffer.from(response.data).toString("base64");
    console.log('Converting image to base 64 success', base64Image)
    return base64Image;
  } catch (error) {
    console.error("Error downloading the image:", imageUrl, error );
    return null;
  }
};

export default imageUrlToBase64;