package collections.test;

import collections.classes.Product;

import java.util.LinkedHashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class NavigableSetTest {
    public static void main(String... args){
        NavigableSet<Product> productNavigableSet = new TreeSet<>();//ordered version
        productNavigableSet.add(new Product("213", "notebook lenovo", 2.500));
        productNavigableSet.add(new Product("211", "notebook vaio", 3.888));
        productNavigableSet.add(new Product("118", "smartphone apple", 3.500));
        productNavigableSet.add(new Product("154", "tv sony", 2.700));
        productNavigableSet.add(new Product("210", "tv lenovo", 2.154));
        productNavigableSet.add(new Product("210", "tv lenovo", 2.600));
        Product product = new Product("210", "tv lenovo", 2.600);
        productNavigableSet.forEach(System.out::println);
        System.out.println(productNavigableSet.ceiling(product));
        System.out.println(productNavigableSet.floor(product));
        System.out.println(productNavigableSet.lower(product));
        System.out.println(productNavigableSet.higher(product));
    }
}