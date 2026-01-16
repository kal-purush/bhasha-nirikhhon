import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { map } from 'rxjs/operators';

import { DataResponseDto } from './dto/data.response.dto';

@Injectable({
  providedIn: 'root',
})
export class MedicService {
  private _numberRandom: number = 0;
  private dataProduct: any = null;

  constructor(private readonly http: HttpClient) {}

  toString() {
    return 'MedicService';
  }

  generateNumber() {
    this._numberRandom = Math.random();
  }

  get numberRandom() {
    return this._numberRandom;
  }

  getProductById(id: number) {
    return this.http
      .get<DataResponseDto>(`https://dummyjson.com/products/${id}`)
      .pipe(
        map((data: DataResponseDto) => ({
          titulo: data.title,
          descripcion: data.description,
          imagen: data.thumbnail,
        }))
      );

    /*   const observable = new Observable((observer: Observer<any>) => {
      fetch(`https://dummyjson.com/products/${id}`)
        .then((response) => response.json())
        .then((data) => {
          observer.next(data);
        });
    });

    return observable; */
  }

  getDetailProduct() {
    return this.dataProduct;
  }
}