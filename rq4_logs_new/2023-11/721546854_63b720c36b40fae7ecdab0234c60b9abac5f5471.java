package n2exercici1;
import n1exercici3.Mapa;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        Restaurant rest = new Restaurant("Rest", 2);
        Restaurant rest1 = new Restaurant("Rest", 2);
        Restaurant rest2 = new Restaurant("Resta", 2);
        HashSet<Restaurant> restaurants = new HashSet<>();
        restaurants.add(rest);
        restaurants.add(rest1);
        restaurants.add(rest2);
    }


}