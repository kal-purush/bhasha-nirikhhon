import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import { environment } from '../environments/environment'


@Injectable()

export class IngredientsService {

  constructor(private http: Http) {}

  getIngredients() {
    return this.http.get(`${environment.BASE_URL}/api/ingredients`)
      .map((res) => {return res.json()});
  }

  getIngredient(id) {
    return this.http.get(`${environment.BASE_URL}/api/ingredients/${id}`)
      .map((res) => res.json());
  }
  
}