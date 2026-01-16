package homework.lab1;

public class ArraySuperIntegerList implements SuperIntegerList {
    private int[] integers;

    public ArraySuperIntegerList(int[] initIntegers) {
        this.integers = initIntegers;
    }

    @Override
    public void add(int number) {
        var integersWithAdd = new int[integers.length + 1];

        System.arraycopy(integers, 0, integersWithAdd, 0, integers.length);
        integersWithAdd[integers.length] = number;

        integers = integersWithAdd;
    }

    @Override
    public void removeByIndex(int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }

        if (index >= integers.length) {
            return;
        }

        var integersWithRemove = new int[integers.length - 1];
        int integersWithRemoveIndex = 0;
        for (int i = 0; i < integers.length; i++) {
            if (index == i) {
                continue;
            }

            integersWithRemove[integersWithRemoveIndex] = integers[i];
            integersWithRemoveIndex++;
        }

        integers = integersWithRemove;
    }

    @Override
    public void removeByValue(int value) {
        boolean isValueFound;

        do {
            // Remove all values in array
            isValueFound = false;

            for (int i = 0; i < integers.length; i++) {
                if (integers[i] == value) {
                    this.removeByIndex(i);
                    isValueFound = true;
                    break;
                }
            }
        } while (!isValueFound);
    }

    @Override
    public int get(int index) {
        if (index >= integers.length) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return integers[index];
    }

    @Override
    public void printAll() {
        for (int integer : integers) {
            System.out.println(integer);
        }
    }
}