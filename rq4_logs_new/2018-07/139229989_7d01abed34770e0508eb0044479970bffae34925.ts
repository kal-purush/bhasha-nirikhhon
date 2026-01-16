import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {RecipeService} from '../recipes/recipe.service';
import 'rxjs-compat/add/operator/map';
import {Recipe} from '../recipes/recipe.model';

@Injectable()
export class DataStorageService {

  constructor(private httpClient: HttpClient,
              private recipeService: RecipeService) {

  }

  public storeRecipes() {
    return this.httpClient.put('https://angular-demo-m.firebaseio.com/recipes.json', this.recipeService.getRecipes());
  }

  public getRecipes(){
    this.httpClient.get('https://angular-demo-m.firebaseio.com/recipes.json')
      .map((response: Recipe[]) => {
        response.forEach(recipe => {
          if (!recipe['ingredients']) {
            recipe['ingredients'] = [];
          }
        });
        return response;
      })
      .subscribe(recipes =>
        this.recipeService.setRecipes(recipes));
  }

}