import { message } from "antd";

export function validateImage(file: any) {
  console.log(file);

  const invalidSizeMessage = "The image size should be less than 4MB.";
  const invalidTypeMessage = "The selected image format is invalid.";
  const size = 4;

  //check type of the uploaded image
  const isJpgOrPng = file.type === "image/jpeg" || file.type === "image/png";
  if (!isJpgOrPng) {
    message.error(invalidTypeMessage);
  }
  //check size of the uploaded image
  const invalidVolume = file.size / 1024 / 1024 < size;
  if (!invalidVolume) {
    message.error(invalidSizeMessage);
  }
  return isJpgOrPng && invalidVolume;
}