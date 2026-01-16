import * as fs from "fs";
import FormData from "form-data";

export const getBufferFromFormData = (
  formData: FormData,
): [filePath: string, Buffer: Buffer] => {
  // Get the image file from FormData (Node.js form-data)
  const imageField = (formData as any)._streams?.find?.(
    (stream: any) =>
      typeof stream === "object" && stream.hasOwnProperty("path"),
  );
  if (!imageField) {
    throw new Error("Image file not found in form data");
  }

  // Read the file as Buffer
  const filePath = imageField.path;
  const buffer = fs.readFileSync(filePath);
  return [filePath, buffer as Buffer];
};