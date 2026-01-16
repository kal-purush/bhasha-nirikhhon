import { Request, Response, NextFunction } from "express";
import GalleryModel from "../models/galleryModel";
import upload from "../middlewares/multerMiddlware";

export const createGallery = async (req: Request, res: Response, next: NextFunction) => {
  upload(req, res, async (err: any) => {
    if (err) {
      return res.status(400).json({ error: "Erreur lors du téléchargement des images." });
    }

    const { title, startDate, endDate } = req.body;
    const files = req.files as Express.Multer.File[];

    if (!title || !startDate || !endDate || files.length === 0) {
      return res.status(400).json({
        error: "Veuillez remplir tous les champs et télécharger au moins une image.",
      });
    }

    const images = files.map((file) => `${req.protocol}://${req.get("host")}/images/${file.filename}`);

    try {
      const gallery = new GalleryModel({ title, startDate, endDate, images });
      await gallery.save();
      res.status(201).json({ message: "Galerie créée avec succès !", gallery });
    } catch (error) {
      res.status(500).json({ error: "Erreur serveur, veuillez réessayer plus tard." });
    }
  });
};

export const getAllGalleries = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const galleries = await GalleryModel.find();
    res.status(200).json(galleries);
  } catch (error) {
    res.status(500).json({ error: "Erreur serveur, veuillez réessayer plus tard." });
  }
};