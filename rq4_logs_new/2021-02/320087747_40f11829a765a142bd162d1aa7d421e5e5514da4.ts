import { Ingredient } from '../../../interfaces/Recipe';
import { AbstractQuantityConversionRule } from '../normalization/AbstractQuantityConversionRule';

export class TableSpoonToGram extends AbstractQuantityConversionRule {
    canHandle(ingredient: Ingredient): boolean {
        return ingredient.quantity_description === 'theelepel'
    }
    
    doHandle(ingredient: Ingredient): Ingredient {
        return {
            ...ingredient,
            quantity_description: 'gram',
            quantity_number: ingredient.quantity_number! * 15
        };
    }
}