import { Router } from "express";
import { PrismaClient } from "@prisma/client";

const router = Router();
const prisma = new PrismaClient();

router.get('/product', async(req, res) => {
  try{
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 50;

    if(page < 1 ) {
      return res.status(400).json({ message: 'You cannot enter a page number less than 1' })
    }

    if(limit < 1 || limit > 100) {
      return res.status(400).json({ message: 'In the limit of minimum 1 and maximum 100' })
    }

    const products = await prisma.sE_Product.findMany({
      skip: (page - 1) * limit,
      take: limit
    });

    res.json({
      paging: {
        page: page,
        limit: limit
      },
      result: products
    })
  }
  catch (error) {
    return res.status(500).json({ message: 'Something went wrong', error: error });
  }
});

router.get('/product/:id', async(req, res) => {
  try{
    if(!req.params.id) {
      return res.status(400).json({ message: 'A parameter was expected' });
    }

    const existsProduct = await prisma.sE_Product.findUnique({
      where: {
        id: req.params.id
      }
    });

    if(!existsProduct) {
      return res.status(404).json({ message: 'Product does not exists' });
    }

    res.json({ result: existsProduct });
  }
  catch (error) {
    return res.status(500).json({ message: 'Something went wrong', error: error });
  }
});

router.get('/products/count', async(req, res) => {
  try{
    const total = await prisma.sE_Product.count();

    res.json({ result: total });
  }
  catch (error) {
    return res.status(500).json({ message: 'Something went wrong', error: error });
  }
});

router.post('/product', async(req, res) => {
  try{
    if(Object.keys(req.body) < 1){
      return res.status(400).json({ message: 'A body was expected' });
    }

    // const {
    //   title,
    //   description,
    //   status,
    //   reference,
    //   idSubCategory,
    //   idCategory,
    //   idBrand
    // } = req.body;

    // const body = Object.keys(req.body);

    // const isEmpty = body.filter(field => {
    //   const value = req.body[field];

    //   return value === undefined || value === null || value === ''
    // });

    // const {
    //   variant_title,
    //   sku,
    //   barcode,
    //   variant_status,
    //   inventoryTracking,
    //   InheritProductPrice,
    //   idOption
    // } = req.body?.variants;

    // if(!title || !description || !status || !reference || !idSubCategory || !idCategory || !idBrand) {

    // }
  }
  catch (error) {
    return res.status(500).json({ message: 'Something went wrong', error: error });
  }
});

export default router;