import { Request, Response } from "express";
import Categories from "../models/categoryModel";
import { IReqAuth } from "../config/interface";

const categoryCtrl = {
  createCategory: async (req: IReqAuth, res: Response) => {
    if (!req.user)
      return res.status(400).json({ msg: "Invalid Authentication" });
    if (req.user.role !== "admin")
      return res.status(400).json({ msg: "You Are Not Authorised" });
    try {
      const name = req.body?.name?.toLowerCase();
      if (!name)
        return res.status(400).json({ msg: "Please provide valid name" });
      const newCategory = new Categories({ name });
      await newCategory.save();
      res.json({ newCategory });
    } catch (err: any) {
      let errMsg;

      if (err.code === 11000) {
        errMsg = Object.values(err.keyValue)[0] + " already exists.";
      } else {
        let name = Object.keys(err.errors)[0];
        errMsg = err.errors[`${name}`].message;
      }

      return res.status(500).json({ msg: errMsg });
    }
  },
  getCategories: async (req: Request, res: Response) => {
    try {
      const categories = await Categories.find().sort("-createdAt");
      res.json({ categories });
    } catch (err: any) {
      res.status(500).json({ msg: err.message });
    }
  },
  updateCategory: async (req: IReqAuth, res: Response) => {
    if (!req.user)
      return res.status(400).json({ msg: "Invalid Authentication" });
    if (req.user.role !== "admin")
      return res.status(400).json({ msg: "Invalid Authentication" });
    try {
      const { name } = req.body;
      const { id } = req.params;
      const category = await Categories.findOneAndUpdate({ _id: id }, { name });
      res.json({ msg: "Updated SuccessFully!" });
    } catch (err: any) {
      res.status(500).json({ msg: err.message });
    }
  },
  deleteCategory: async (req: IReqAuth, res: Response) => {
    if (!req.user)
      return res.status(400).json({ msg: "Invalid Authentication." });
    if (req.user.role !== "admin")
      return res.status(400).json({ msg: "Invalid Authentication." });
    try {
      const { id } = req.params;
      await Categories.findByIdAndDelete(id);
      res.json({ msg: "Deleted SuccessFully!" });
    } catch (err: any) {
      res.status(500).json({ msg: err.message });
    }
  },
};

export default categoryCtrl;