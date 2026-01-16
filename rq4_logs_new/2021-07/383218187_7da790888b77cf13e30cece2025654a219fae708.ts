import express, { Response, Request, Router, NextFunction } from "express";
import { dataAirbnb } from "../../db";
import { paginado } from "../paginado";
import { Properties } from "../models/Properties";
import { Reserva } from "../models/Users";
import { Propertiestests } from "../models/propertiestests";
import multer from "multer";
import { any } from "sequelize/types/lib/operators";

const path = require("path");
// import cors from "cors";
// import config from "../lib/config";
const storage = multer.diskStorage({
  destination: "public/uploads",
  filename: (req, file, cb) => {
    cb(null, file.originalname);
  },
});
const router = Router();

router.use(
  multer({
    storage,
    dest: "public/uploads",
    fileFilter: (req, file, cb: any) => {
      const filetypes = /jpeg|jpg|png|svg/;
      const mimetype = filetypes.test(file.mimetype);
      const extname = filetypes.test(path.extname(file.originalname));
      if (mimetype && extname) {
        return cb(null, true);
      }
      cb(" Archivo debe ser imagen valida");
    },
  }).single("image")
);

router.post("/", (req: any, res: Response, next: NextFunction) => {
  console.log(req.file.path);
  res.json(req.body);
});

export default router;