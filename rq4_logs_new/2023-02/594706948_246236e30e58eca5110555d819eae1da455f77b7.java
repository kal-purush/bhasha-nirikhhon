package src.main.java.homework5.figures;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {


        ArrayList<Area> list = new ArrayList<Area>();

        Area triangle1 = new Triangle (10,20.0006,20.96668);
        list.add(triangle1);

        Area triangle2 = new Triangle (11,25.00,14.966);
        list.add(triangle2);

        Area triangle3 = new Triangle (102,205.66,104.968);
        list.add(triangle3);

        Area circle1 = new Circle (12.96);
        list.add(circle1);

        Area circle2 = new Circle (51.0);
        list.add(circle2);

        Area square = new Square (98);
        list.add(square);

        double squareSum = 0;
        for (int i = 0; i < list.size(); i++) {

            squareSum += list.get(i).square();

        }

        System.out.println("Total area = " + squareSum);

    }


}