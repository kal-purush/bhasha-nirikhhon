package com.Animal;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Mike on 1/12/2016.
 */
public class App {
    public static void main(String[] args) {
        Animal bob = new Dog(1, 10, 20, "white", "Bob", true, true);
        // bob.voice();
//        System.out.println();
        Cat puss = new Cat(2, 10, 20, "gray", "Barsik", true);
//        puss.voice();
//        System.out.println();
        Wild giraffe = new Wild(3, 5, 100, "green", false);
//        giraffe.voice();
//        System.out.println();
        Wild croc = new Wild(4, 5, 100, "green", true);
//        croc.voice();
//        System.out.println();
//        Fish nemo=new Fish(1,2,3,"gold","Nemo",true);
//        nemo.voice();


        List<Animal> arr = new ArrayList();
        arr.add(bob);
        arr.add(puss);
        arr.add(giraffe);

        for (int i = 0; i < arr.size(); i++) {
            arr.get(i).voice();

        }
    }

}