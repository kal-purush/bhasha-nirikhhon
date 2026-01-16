package ee.tlu.taavim.massiivid;

import java.util.Arrays;

public class Massicid {

    public static void main(String[] args) {
        String[] autod = {"Ford", "BMW", "lada"};
        //Massiivi läbikäimine
        for (String auto : autod) {
            System.out.println(auto);
        }
        //Ford
        //BMW
        //lada

        System.out.println("");
        //Massiivi elemendi muutmine
        for (String auto : autod) {
            if (auto.equals("lada")) {
                auto = "Lada";
            }
            System.out.println(auto);
        }
        //Ford
        //BMW
        //Lada

        /* massiivi saab ka objekte salvestada
        
        Ioon[] ioon = new Ioon[3];
        ioon[0] = new Ioon("Ca", 2, 2);
        ioon[1] = new Ioon("Cl", 1, -1);
        ioon[2] = new Ioon("Cl", 1, -1);
        
         */
        //Massiive saab ka võrrelda
        String[] autod2 = {"Ford", "BMW", "lada"};
        if (Arrays.equals(autod2, autod)) {
            System.out.println("massiivid: autod ja autod2 on täpselt samad ehk võrdsed!");
        } else {
            System.out.println("massiivid: autod ja autod2 ei ole täpselt samad!");
        }
    }
}