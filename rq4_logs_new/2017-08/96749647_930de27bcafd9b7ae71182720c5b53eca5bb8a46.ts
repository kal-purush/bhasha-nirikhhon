import { Validators } from '@angular/forms';

import { IFormEntity } from 'app/contract/IFormEntity';
import { IRecipeType } from 'app/contract/IRecipeType';

export class RecipeType implements IFormEntity, IRecipeType {
  public id: number;
  public name: string;

  public formErrors = {
    'name': ''
  };

  public validationMessages = {
    'name': {
      'required': 'Name is required.',
    }
  };

  constructor(source?: IRecipeType) {
    if (source) {
      this.id = source.id;
      this.name = source.name;
    }
  }

  public getFormConfig(): any {
    return {
      'name': [this.name, [Validators.required]]
    }
  }
}