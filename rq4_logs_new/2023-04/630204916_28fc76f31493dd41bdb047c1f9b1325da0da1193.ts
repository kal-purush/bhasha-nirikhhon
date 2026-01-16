import { Request, Response } from 'express';
import { db } from '../../connection/connection';
import { Products } from '../../schema/products';

export const newProduct = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: Products = req.body;
    const docRef = await db.collection("products").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al crear un nuevo producto');
  }
};

export const updateProduct = async (req: Request, res: Response): Promise<void> => {
  try {
    const updatedProductData: Products = req.body;
    const productName: string = req.params.productName;
    await db.collection('products').doc(productName).update(updatedProductData);
    res.status(200).json(updatedProductData);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al actualizar el producto');
  }
};
