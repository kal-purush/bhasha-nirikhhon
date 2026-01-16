import { RecipesActions } from '../actions/types';
import { SingleRecipeItem } from '../../commons/interfaces/single-recipe-item.interface';

export interface State {
    products: Array<SingleRecipeItem>;
    sport: Array<SingleRecipeItem>;
}

export const initialState: State = {
    products: [],
    sport: [],
};

export function recipesReducer(
    state = initialState,
    action: RecipesActions,
): State {
    switch (action.type) {
        default: {
            return state;
        }
    }
}
