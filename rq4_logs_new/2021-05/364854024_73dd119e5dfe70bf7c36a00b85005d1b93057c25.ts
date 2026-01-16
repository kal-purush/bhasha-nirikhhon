import { ProductCategory } from "./enums/product-category.enum";

export class Product {
    id: number;
    name: string;
    category: ProductCategory;
    description: string;
    photo: string;
    price: number;

    constructor(item?: any) {
        this.id = item?.id || null;
        this.name = item?.name || '';
        this.category = item?.category || null;
        this.description = item?.description || 0;
        this.photo = item?.photo || '';
        this.price = item?.price || 0;
    }
}