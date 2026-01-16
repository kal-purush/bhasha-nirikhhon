package hw1;

import hw1.fruits.Fruit;

import java.util.ArrayList;

public class BoxWithFruits<T extends Fruit> {
    private ArrayList<T> fruits = new ArrayList();

    private float weight;
    private T t = null;

    @Override
    public String toString() {
        return "BoxWithFruits{" +
                "fruits=" + fruits.size() +
                ", weight=" + weight +
                '}';
    }

    public float getWeight() {
        return weight;
    }

    public void addFruitInBox(T fruit, int num) {

        for (int i=0;i<num;i++)
            fruits.add(fruit);
       weight+=fruit.getWeight()*num;
    }

    public void moveTo(BoxWithFruits<T> new_box) {
            for (T fruit : fruits) {
                new_box.addFruitInBox(fruit,1);
            }
        fruits.removeAll(fruits);
        this.weight = 0;
    }


    public boolean compareByWeight(BoxWithFruits<?> b2) {
        if(getWeight() == b2.getWeight()) return true;
        return false;
    }

    public boolean compareByType(BoxWithFruits<?> b2) {
        if(this.getClass() == b2.getClass()) return true;
        return false;
    }

    public boolean compareByTypeAndWeight(BoxWithFruits<T> b2) {
        // типы должны совпадать
        if(getWeight() == b2.getWeight()) return true;
        return false;
    }
}