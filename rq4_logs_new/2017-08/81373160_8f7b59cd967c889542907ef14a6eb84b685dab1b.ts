import { IRecipes } from '../models/recipes';
import * as recipes from '../actions/recipes';

export type State = IRecipes[];

const initialState: State = [{
  id: 3,
  title: 'דושפרה: מרק בוכרי שהוא ארוחה שלמה',
  image_path: 'data/recipes/img/dushpara.jpg',
  rating: 4.5,
  description: 'דושפרה הוא אחד המרקים הנפוצים והמוכרים במטבח הבוכרי. כמרק מחמם ומשביע הוא מציע, מעבר לכיסונים שמעניקים לו את שמו, גם ',
  prep_time: '2.5 שעות',
  portions: 6,
  calories: 465
}];

export function reducer(state = initialState, action: recipes.Actions): State {
  switch (action.type) {
    case recipes.LOAD_RECIPES: {
      return initialState;
    }

    case recipes.LOAD_RECIPES_SUCCESS: {
      return action.payload;
    }

    default: {
      return state;
    }
  }
}