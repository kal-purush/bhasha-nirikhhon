package Lesson3;

import Lesson3.drugs.Component;
import Lesson3.drugs.impl.CatDrug;
import Lesson3.drugs.impl.components.Azitron;
import Lesson3.drugs.impl.components.Pinecilin;
import Lesson3.drugs.impl.components.Water;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Component> componentsCat = List.of(new Azitron("Азитрон", "2", 4),
                new Water("Вода", "15", 1));
        CatDrug drug = new CatDrug(componentsCat);
        for (Component component : drug) {
            System.out.println(component);
        }
        List<Component> componentCat2 = List.of(new Azitron("Азитрон","4",4),
                new Pinecilin("Пинецилин", "18", 6));
        List<Component> componentCat3 = List.of(new Water("Вода", "20", 12),
                new Azitron("Азитрон", "10", 9));
        List<Component> componentCat4 = List.of(new Water("Вода", "20", 12),
                new Azitron("Азитрон", "10", 100));
        CatDrug drug1 = new CatDrug(componentCat2);
        CatDrug drug2 = new CatDrug(componentCat3);
        CatDrug drug4 = new CatDrug(componentCat4);

        List<CatDrug> drugs = new ArrayList<>();
        drugs.add(drug);
        drugs.add(drug1);
        drugs.add(drug2);
        drugs.add(drug4);
//        System.out.println(drugs);

        Collections.sort(drugs);
        System.out.println(drugs);
    }
}