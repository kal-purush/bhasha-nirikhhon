import { ProductDto } from '../../src/dtos/ProductDto';
import { ILocalOrder } from '../../src/context/OrderContext';
import { LineItemDto } from '../../src/dtos/LineItemDto';
import { CategoryDto } from '../../src/dtos/CategoryDto';

export const product: ProductDto = {
  name: 'This is a product',
  category: {
    name: 'a',
    description: 'a category',
    id: 1,
  },
  tags: [1],
  description: 'Product',
  id: 1,
  price: 500,
  retired: false,
};

export const secondProduct: ProductDto = {
  name: 'This is another product',
  category: {
    name: 'a',
    description: 'a category',
    id: 1,
  },
  tags: [1],
  description: 'Product',
  id: 1,
  price: 750,
  retired: false,
};

export const longDescriptionProduct: ProductDto = {
  name: 'This is another product',
  category: {
    name: 'a',
    description: 'a category',
    id: 1,
  },
  tags: [1],
  description: '5fVCrhLFoTk62QQcgBXjQg9gHdkh1MxEtOXPL9pDHGjcMfkdh8aaaaaa',
  id: 1,
  price: 750,
  retired: false,
};

export const products: ProductDto[] = [product, secondProduct];

export const categories: CategoryDto[] = [
  {
    id: 1,
    name: 'gluten-free',
    description: 'A category for all gluten-free products',
    products: [4, 5, 6],
    retired: false,
  },
  {
    id: 2,
    name: 'Organic',
    description: 'A category for all organic products',
    products: [7, 8, 9],
    retired: false,
  },
  {
    id: 3,
    name: 'Vegetarian',
    description: 'A category for all vegetarian products',
    products: [10, 11, 12],
    retired: false,
  },
];

export const lineItem = { product: product, quantity: 5, subTotal: 2500 };
const lineItemTwo: LineItemDto = { product: secondProduct, quantity: 1, subTotal: 750 };

export const lineItemList: LineItemDto[] = [];
lineItemList.push(lineItem);
lineItemList.push(lineItemTwo);

export const localOrderData = { id: 1, lineItems: lineItemList };

export function getExampleOrder(): ILocalOrder {
  return {
    localOrder: localOrderData,
    addToLocalOrder(product: ProductDto): void {
      expect(product.name).to.equal('This is a product');
    },
    removeLocalOrder(): void {
      return;
    },
    removeProduct(product: ProductDto): void {
      expect(product.name).to.equal('This is a product');
    },
    setOrderId(id: number): void {
      return;
    },
  };
}