public class Calculation {

    int count (int distanceInKm){
        int boardingCostInRubles = 60;
        int costOfKilometerInRubles = 20;
        int discountInPercent = 5;
        int discountLimitInRubles = 100;
        int discountStartInRubles = 1000;
        int discountInRubles;

        int amount = boardingCostInRubles + (costOfKilometerInRubles * distanceInKm);

        if (amount > discountStartInRubles) {
            discountInRubles = (amount * discountInPercent / 100);
            if (discountInRubles > discountLimitInRubles) {
                discountInRubles = discountLimitInRubles;
            }
        }
        else {
            discountInRubles = 0;
        }
        int result = amount - discountInRubles;
        return result;
    }
}