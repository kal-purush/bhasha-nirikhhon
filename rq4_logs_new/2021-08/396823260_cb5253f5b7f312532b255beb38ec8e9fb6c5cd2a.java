package homework.lab1;

public class Main {
    public static void main(String[] args) {
        SuperIntegerList superIntegerList = new ArraySuperIntegerList(new int[]{1, 2, 3});

        superIntegerList.printAll(); // should be 1 2 3

        System.out.println();

        superIntegerList.removeByIndex(1);
        superIntegerList.add(5);
        superIntegerList.add(6);
        superIntegerList.printAll(); // should be 1 3 5 6

        System.out.println();

        superIntegerList.add(6);
        superIntegerList.removeByValue(6);
        superIntegerList.printAll(); // should be 1 3 5

        System.out.println();

        superIntegerList.removeByIndex(0);
        superIntegerList.removeByIndex(0);
        System.out.println(superIntegerList.get(0)); // should be 5

        try {
            superIntegerList.removeByIndex(-5);
        } catch (IllegalArgumentException illegalArgumentException) {
            System.out.println("Can't remove by negative index");
        }

        try {
            superIntegerList.get(10);
        } catch (ArrayIndexOutOfBoundsException boundsException) {
            System.out.println("Can't get value that out of bounds");
        }

    }
}