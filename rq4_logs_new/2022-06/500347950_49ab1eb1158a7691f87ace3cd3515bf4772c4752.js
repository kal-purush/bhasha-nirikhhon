const Fruit = require('./Fruit');
const FruitBasket = require('./FruitBasket');

class BasketCalculation {
    constructor(fruitsData) {
        this.fruitsData = fruitsData;
    }

    createSummary() {
        return this.fruitsData.map(fruitData => {
           const fruitBasketObj = new FruitBasket(fruitData.id, fruitData.max_weight, fruitData.contents);
           const fruitContentSummary = [];
           let fruitTotalWeight = 0;
           fruitBasketObj.getFruitContents().map(content => {
               const fruitObj = new Fruit(content.type, content.color, content.weight);
               const fruitType = fruitObj.getType();
               const fruitContentTypeExist = fruitContentSummary.find(summary => summary.type === fruitType);
               if (!fruitContentTypeExist) {
                   fruitContentSummary.push({type: fruitType, count: 1})
               } else {
                   fruitContentSummary.count++;
               }
               fruitTotalWeight = fruitTotalWeight + fruitObj.getWeight();
           });
            return {
                id: fruitBasketObj.getFruitId(),
                total_fruits: fruitBasketObj.getFruitContents().length,
                total_weight: fruitTotalWeight,
                fruit_counts: fruitContentSummary
            }
       })
    }
}

module.exports = BasketCalculation;