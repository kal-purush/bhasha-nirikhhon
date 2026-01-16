import multer, { StorageEngine } from "multer";
import { Request } from "express";

const MIME_TYPES: { [key: string]: string } = {
  "image/jpg": "jpg",
  "image/jpeg": "jpeg",
  "image/png": "png",
};

const storage: StorageEngine = multer.diskStorage({
  destination: (
    req: Request,
    file: Express.Multer.File,
    callback: (error: Error | null, destination: string) => void
  ) => {
    callback(null, "images");
  },
  filename: (
    req: Request,
    file: Express.Multer.File,
    callback: (error: Error | null, filename: string) => void
  ) => {
    const extension = MIME_TYPES[file.mimetype];
    callback(
      null,
      `${file.originalname.split(" ").join("_")}_${Date.now()}.${extension}`
    );
  },
});

const singleUpload = multer({ storage }).single("image");

export default singleUpload;