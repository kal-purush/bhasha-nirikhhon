import { combineToSingleValue } from '../../components/shopping-list/normalization/Combiner';
import { RulesHandler } from '../../components/shopping-list/normalization/RulesHandler';
import { SortedRecipeMap } from '../../components/shopping-list/normalization/SortRecipeMap';
import { Ingredient } from '../../interfaces/Recipe';

const input: SortedRecipeMap = {
    apple: [{
      name: 'apple',
      quantity_number: 1,
      quantity_description: 'stuk'  
    }, {
        name: 'apple',
        quantity_number: 2,
        quantity_description: 'stuk'
    }],
    floor: [{
        name: 'floor',
        quantity_number: 500,
        quantity_description: 'gram'
    }, {
        name: 'floor',
        quantity_number: 750,
        quantity_description: 'gram'
    }]
}

const rulesHandler = new RulesHandler([]);
describe('combineToSingleValue', () => {
    describe('passing apples and floor', () => {
        let ingredients: Ingredient[];
        beforeEach(() => {
            ingredients = combineToSingleValue(input, rulesHandler);
        });

        test('returns array of two entries', () => {
            expect(ingredients.length).toBe(2);
        });

        test('combines apples correctly', () => {
            expect(ingredients[0]).toEqual({
                name: 'apple',
                quantity_description: 'stuk',
                quantity_number: 3
            });
        });

        test('combines floor correctly', () => {
            expect(ingredients[1]).toEqual({
                name: 'floor',
                quantity_description: 'gram',
                quantity_number: 1250
            });
        });
    });
});