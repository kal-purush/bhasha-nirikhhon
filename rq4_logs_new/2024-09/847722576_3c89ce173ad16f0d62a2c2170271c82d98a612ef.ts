import { Product, CatalogProductAPI } from "@/app/interface/ProductInterface";

export const cleanProductObjects = (
  objects: CatalogProductAPI[]
): Product[] => {
  const cleanedObjects: Product[] = [];
  objects.forEach((object) => {
    console.log(object);
    // const cleanedPrice = object.itemData?.variations?.[0]?.itemVariationData
    //   ?.priceMoney?.amount
    //   ? Number(
    //       object.itemData.variations[0].itemVariationData.priceMoney.amount
    //     ) / 100
    //   : 0;
    cleanedObjects.push({
      id: object.catalogObjectId,
      name: object.name,
      price: object.variations[0]?.price?.amount ?? 0,
      image:
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRjw-x2av0YFJfxJx6oN6lOQqC3TxftSOqtKA&s",
    });
  });
  return cleanedObjects;
};