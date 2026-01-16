import { NextFunction, Request, Response } from "express";
import { upload } from "../application/multer";
import { ResponseError } from "../error/response-error";

export const uploadImage = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  upload(req, res, (err) => {
    if (err) {
      if (err.message === "Only image file are allowed") {
        return next(err);
      }

      if (err.code === "LIMIT_FILE_SIZE") {
        return next(new ResponseError(400, "Maximum limit image size is 2MB"));
      }
      return res.redirect("/settings");
    }

    if (!req.file) {
      return next(new ResponseError(400, "Image file required"));
    }

    next();
  });
};