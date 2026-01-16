import { Ingredient } from '../../../interfaces/Recipe';
import { TableSpoonToGramRule } from './rules/TableSpoonToGramRule';
import { TeaSpoonToGramRule } from './rules/TeaSpoonToGramRule';
import { RulesHandler } from './RulesHandler';
import { SortedRecipeMap } from './SortRecipeMap';

const rulesHandler = new RulesHandler([
    new TableSpoonToGramRule(), 
    new TeaSpoonToGramRule()
]);

export function combineToSingleValue(sortedRecipeMap: SortedRecipeMap): Ingredient[] {
    const keys = Object.keys(sortedRecipeMap);
    return keys.map((key) => {
        const recipeMap: Ingredient[] = sortedRecipeMap[key];
        const ingredients = rulesHandler.normalize(recipeMap);
        return {
            name: key,
            quantity_number: ingredients.reduce((previous, current) => {
                return previous + current.quantity_number!;
            }, 0),
            quantity_description: ingredients[0].quantity_description
        }
    })
}