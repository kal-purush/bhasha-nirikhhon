import { Injectable } from '@angular/core';
import { DocumentReference } from '@angular/fire/firestore';
import { Observable } from 'rxjs';

import { FirebaseService } from '../../../core/services/firebase.service';
import { IngredientUnits } from '../models/ingredient-units.interface';

@Injectable({
  providedIn: 'root'
})
export class IngredientsUnitsService {
  private readonly collection = 'ingredients-units';

  constructor(private firebaseService: FirebaseService) { }

  public get(): Observable<IngredientUnits[]> {
    return this.firebaseService.getCollectionData<IngredientUnits>(this.collection);
  }

  public post(ingredient: IngredientUnits): Promise<DocumentReference<IngredientUnits>> {
    return this.firebaseService.postDocument(this.collection, ingredient);
  }

  public update(ingredientUpdated: IngredientUnits): Promise<void> {
    return this.firebaseService.updateDocument(this.collection, ingredientUpdated);
  }

  public delete(ingredientToDelete: IngredientUnits): Promise<void> {
    return this.firebaseService.deleteDocument(this.collection, ingredientToDelete.id as string);
  }

}