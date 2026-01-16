package com.vartanian.patterns.composite;

import com.vartanian.patterns.composite.items.MenuItem;
import com.vartanian.patterns.composite.menu.Menu;
import com.vartanian.patterns.composite.waiters.Waiter;
import com.vartanian.patterns.templatemethod.beverages.CoffeineBeverageWithHook;
import com.vartanian.patterns.templatemethod.beverages.impl.CoffeWithHook;

import java.util.Arrays;

public class Simulator {

    public static void main(String[] args) {

        MenuItem item1 = new MenuItem("Item1", 0.99);
        MenuItem item2 = new MenuItem("Item2", 0.89);
        MenuItem item3 = new MenuItem("Item3", 0.79);
        MenuItem item4 = new MenuItem("Item4", 0.69);
        MenuItem item5 = new MenuItem("Item5", 0.59);

        Menu menu2 = new Menu("Menu2");
        Menu menu3 = new Menu("Menu3");
        menu2.add(item3);
        menu2.add(item4);
        menu3.add(item5);
        menu2.add(menu3);

        Menu menu1 = new Menu("Menu1");
        menu1.add(item1);
        menu1.add(item2);
        menu1.add(menu2);

        Waiter waiter = new Waiter(menu1);

        waiter.printMenu();

    }

}