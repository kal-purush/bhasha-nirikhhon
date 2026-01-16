import fruits.Apple;
import fruits.FruitBox;
import fruits.Orange;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        Integer[] arr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        ChangeElements<Integer> myNameArr = new ChangeElements<Integer>(arr);
        System.out.println(Arrays.toString(myNameArr.change(3, 8)));

        String[] arrStr = {"qwe", "rty", "uio", "asd"};
        ChangeElements<String> myArrStr = new ChangeElements<String>(arrStr);
        System.out.println(Arrays.toString(myArrStr.change(0, 3)));

        Integer[] arrToList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        ArrayToList<Integer> arrList = new ArrayToList<Integer>(arrToList);
        System.out.println(arrList.getList());


        Apple apple = new Apple();
        Orange orange = new Orange();
        System.out.println(apple.getWeight());
        System.out.println(orange.getWeight());
        FruitBox<Apple> appleBox = new FruitBox<Apple>();
        for (int i = 0; i < 6; i++) {
            appleBox.append(apple);
        }

        System.out.println("In appleBox " + appleBox.getWeight() + " apples");

        FruitBox<Orange> orangeBox = new FruitBox<Orange>();
        for (int i = 0; i < 7; i++) {
            orangeBox.append(orange);
        }

        System.out.println("In orangeBox " + orangeBox.getWeight() + " oranges");
        System.out.println(appleBox.compare(orangeBox));

        FruitBox<Apple> appleBox2 = new FruitBox<Apple>();
        System.out.println("In appleBox " + appleBox.getWeight() + " apples");


        System.out.println("Pour it all in appleBox2!");
        appleBox.pourAll(appleBox2);
        System.out.println("In appleBox " + appleBox.getCount() + " apples");
        System.out.println("In appleBox2 " + appleBox2.getCount() + " apples");

    }

}