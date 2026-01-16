import fetch from 'node-fetch';

export const  downloadAndConvertToBase64 = async(fileUrl: string): Promise<string> => {
  try {
    // 1. download the file from the URL
    const response = await fetch(fileUrl);

    // check if the file is downloaded successfully
    if (!response.ok) {
      throw new Error(`Error while downloading ${fileUrl}: ${response.status} ${response.statusText}`);
    }

    // 2. get file content in a Buffer
    const fileBuffer: Buffer = await response.buffer();

    // 3. Convert Buffer to base64
    const base64String: string = fileBuffer.toString('base64');

    return base64String;
  } catch (error: unknown | Error) {
    console.error("Error while downloading or converting file to base64 :", (error as Error).message);
    return '';
  }
}