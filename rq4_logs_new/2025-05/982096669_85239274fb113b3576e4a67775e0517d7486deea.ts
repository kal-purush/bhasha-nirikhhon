import { Request, Response } from "express";
import * as brandService from "./brand.service";
import {
  CreateBrandDto,
  UpdateBrandDto,
  BrandQueryParams,
} from "./brand.types";

// Create a new brand
export const createBrand = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const brandData: CreateBrandDto = req.body;
    const brand = await brandService.createBrand(brandData);
    res.status(201).json(brand);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

// Get all brands
export const getAllBrands = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const query: BrandQueryParams = {
      page: req.query.page ? parseInt(req.query.page as string) : undefined,
      limit: req.query.limit ? parseInt(req.query.limit as string) : undefined,
      sort: req.query.sort as string,
      name: req.query.name as string,
    };

    const brands = await brandService.getAllBrands(query);
    res.status(200).json(brands);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

// Get brand by ID
export const getBrandById = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const { id } = req.params;
    const brand = await brandService.getBrandById(id);
    res.status(200).json(brand);
  } catch (error) {
    res.status(404).json({ message: error.message });
  }
};

// Update brand by ID
export const updateBrandById = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const { id } = req.params;
    const updateData: UpdateBrandDto = req.body;
    const brand = await brandService.updateBrandById(id, updateData);
    res.status(200).json(brand);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

// Delete brand by ID
export const deleteBrandById = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const { id } = req.params;
    const result = await brandService.deleteBrandById(id);
    res.status(200).json(result);
  } catch (error) {
    res.status(404).json({ message: error.message });
  }
};

// Get products by brand ID
export const getProductsByBrandId = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const { id } = req.params;
    const query = {
      page: req.query.page ? parseInt(req.query.page as string) : 1,
      limit: req.query.limit ? parseInt(req.query.limit as string) : 10,
    };

    const products = await brandService.getProductsByBrandId(id, query);
    res.status(200).json(products);
  } catch (error) {
    res.status(404).json({ message: error.message });
  }
};