import { SerializationHelper } from './serialization.helper';
import { Product } from '../_model/product.model';

export class JsonConverter<T> {

  constructor(private obj: T) { }

  public convertJsonToObjectsArray(_data: any[]): T[] {
    const products: T[] = new Array();
    for (let j = 0; j < _data.length; j = j + 1) {
      products.push(
        SerializationHelper.toInstance<T>(this.obj,
          JSON.stringify(_data[j])) // '{ "code": "code1", "description": "desc1", "id": "1" }'
      );

    }
    return products;
  }
}