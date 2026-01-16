

import { Exclude } from "class-transformer";
import { CartEntity } from "src/entity/entities/cart.entity";
import { ProductEntity } from "src/entity/entities/product.entity";



export class GetCartSerializer {
   
    quantity: number;
    
    product: ProductEntity;

    constructor(data: CartEntity[]) {

    }
}