package hw.lesson5;

import java.util.ArrayList;
import java.util.List;

public class Rucksack {

    private int theBestPrice;

    private final int maxWeight;

    protected Rucksack(int maxWeight) {
        this.maxWeight = maxWeight;
    }

    protected int getTheBestPrice() {
        return theBestPrice;
    }

    private List<Thing> theBestThings;

    private int countPrice (List<Thing> things){
        int resultPrice = 0;
        for (Thing thing : things) {
            resultPrice += thing.getPrice();
        }
        return resultPrice;
    }

    private int countWeight(List<Thing> things){
        int resultWeight = 0;
        for (Thing thing : things) {
            resultWeight += thing.getWeight();
        }
        return resultWeight;
    }

    private void findTheBestSet(List<Thing> things) {
        if ((countPrice(things) > theBestPrice || theBestThings == null)
                && countWeight(things) <= maxWeight){
            theBestPrice = countPrice(things);
            theBestThings = things;
        }
    }

    protected List<Thing> getTheBestSet() {
        return theBestThings;
    }
// method with recursion
    protected void countTheBestSet(List<Thing> things){
        if (things.isEmpty()){
            return;
        }
        findTheBestSet(things);
        for (int i = 0; i < things.size(); i++) {
            List<Thing> newThingsSet = new ArrayList <>(things);
            newThingsSet.remove(i);
            countTheBestSet(newThingsSet);
        }
    }
}