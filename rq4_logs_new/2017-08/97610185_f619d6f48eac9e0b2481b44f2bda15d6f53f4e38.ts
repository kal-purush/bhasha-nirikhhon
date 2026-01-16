import { SerializationHelper } from './serialization.helper';
import { Product } from '../../_model/product.model';

export class ProductHelper {

  public static convertJsonToProductsArray(_data: any[]): Product[] {
    const products: Product[] = new Array();
    for (let j = 0; j < _data.length; j = j + 1) {
      products.push(
        SerializationHelper.toInstance(new Product(),
          JSON.stringify(_data[j])) // '{ "code": "code1", "description": "desc1", "id": "1" }'
      );

    }
    return products;
  }
}