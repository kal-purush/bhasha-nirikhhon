import { sortByIngredient, SortedRecipeMap } from '../../components/shopping-list/normalization/SortRecipeMap';
import { Ingredient } from '../../interfaces/Recipe';

const APPLE_INGREDIENTS: Ingredient[] = [{
    name: 'apple',
    quantity_description: 'stuk',
    quantity_number: 1
},
{
    name: 'apple',
    quantity_description: 'stuk',
    quantity_number: 1
}];

const PEAR_INGREDIENTS: Ingredient[] = [{
    name: 'pear',
    quantity_description: 'stuk',
    quantity_number: 1
}]

const TEST_INGREDIENTS: Ingredient[] = [
    ...APPLE_INGREDIENTS,
    ...PEAR_INGREDIENTS
]

describe('sortByIngredient', () => {
    describe('when called with three ingredients', () => {
        let result: SortedRecipeMap;

        beforeEach(() => {
            result = sortByIngredient(TEST_INGREDIENTS)
        });

        test('should have an apple property of type array', () => {
            expect(Array.isArray(result.apple)).toBe(true);
        });

        test('should have two items in apple property', () => {
            expect(result.apple.length).toBe(2);
        });

        test('should match ingredients in input', () => {
            expect(result.apple[0]).toBe(APPLE_INGREDIENTS[0]);
            expect(result.apple[1]).toBe(APPLE_INGREDIENTS[1]);
        })

        test('should have a pear property of type array', () => {
            expect(Array.isArray(result.pear)).toBe(true);
        });

        test('should have one item in pear property', () => {
            expect(result.pear.length).toBe(1);
        });

        test('should match ingredient in input', () => {
            expect(result.pear[0]).toBe(PEAR_INGREDIENTS[0]);
        });
    })
});