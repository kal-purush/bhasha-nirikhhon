import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

//New Addition
@Injectable()
export class GetRecipeProvider {

    getCalories: number
    constructor(public http: HttpClient) {
        console.log('Hello GetRecipe Provider');
      }

      getRecipe():Observable<any>{
          this.getCalories = 1000;
          return this.http.get("https://api.spoonacular.com/recipes/findByNutrients?maxCalories=600&number=1&apiKey=6b711da039a542a1a9ffc56c373bc784")
            
        }
}