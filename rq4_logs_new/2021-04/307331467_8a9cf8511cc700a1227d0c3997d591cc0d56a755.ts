import { Injectable } from "@angular/core";
import { HttpClient } from '@angular/common/http';
import { RecipeService } from './recipe.service';
import { Recipe } from './recipe.model';
import { map, tap } from 'rxjs/operators';
import {Observable, Subject, Subscription} from 'rxjs';

@Injectable({providedIn: 'root'})
export class DataStorageService {
  private dataBaseUrl = 'https://kitchenapp-ad647.firebaseio.com/recipes.json';

  constructor(private http: HttpClient, private recipeService: RecipeService) {
  }

  storeRecipes() {
    const recipes = this.recipeService.getRecipes();

    this.http
      .put(this.dataBaseUrl, recipes)
      .subscribe(response => {
        console.log(response)
    })
  }

  fetchRecipes(): Observable<Recipe[]> {
    return this.http
      .get<Recipe[]>(this.dataBaseUrl)
      .pipe(
        map(recipes => {
          return recipes.map(recipe => {
            return {
              ...recipe,
              ingredients: recipe.ingredients ? recipe.ingredients : []
            }
          })
        }),
        tap(recipes => {
          this.recipeService.setRecipes(recipes);
        })
      )
  }
}