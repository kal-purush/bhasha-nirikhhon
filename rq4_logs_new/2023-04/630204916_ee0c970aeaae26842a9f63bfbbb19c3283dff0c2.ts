import { Request, Response } from 'express';
import { db } from '../../connection/connection';
import { Products } from '../../schema/products';

export const getAllProducts = async (req: Request, res: Response): Promise<void> => {
  try {
    const productsSnapshot = await db.collection('products').get();

    const products = productsSnapshot.docs.map((doc) => {
      return {
        id: doc.id,
        ...doc.data()
      };
    });

    res.status(200).json(products);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al obtener los productos');
  }
};

export const getAllProductsByType = async (req: Request, res: Response): Promise<void> => {
  try {
    const productType: string = req.params.productType;
    const productsSnapshot = await db.collection('products').where('type', '==', productType).get();

    const products = productsSnapshot.docs.map((doc) => {
      return {
        id: doc.id,
        ...doc.data()
      };
    });

    res.status(200).json(products);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al obtener los productos por tipo');
  }
};

export const getAllProductsByStore = async (req: Request, res: Response): Promise<void> => {
  try {
    const store: string = req.params.store;
    const productsSnapshot = await db.collection('products').where('localId', 'array-contains', store).get();

    const products = productsSnapshot.docs.map((doc) => {
      return {
        id: doc.id,
        ...doc.data()
      };
    });

    res.status(200).json(products);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al obtener los productos por tienda');
  }
};
