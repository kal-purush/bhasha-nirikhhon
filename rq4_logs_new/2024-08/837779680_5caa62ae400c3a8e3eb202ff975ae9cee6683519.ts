import ProductImage from '../db-files/models/ProductImage';

const countProductImages = async(id: string): Promise<number> => {
  const imagesCount = await ProductImage.count({ where: { productId: id } });
  return imagesCount;
};

const createProductImageService = async(
  productId: string,
): Promise<ProductImage | null> => {
  const productImage:ProductImage | null = await ProductImage.create({
    path: './temp', // just a temp path until we retrieve the firebase one.
    productId,
  });
  return productImage;
};

export { countProductImages, createProductImageService };