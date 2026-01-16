import java.util.Arrays;


class upgradeArray {
    private Float[] array;

    upgradeArray() {
        this.array = new Float[0];
    }

    Float[] getArray() {
        return array;
    }

    Float[] add(Float element) {
        Float[] copyArray = new Float[array.length + 1];
        for (int i = 0; i < array.length; i++) {
            copyArray[i] = array[i];
        }
        copyArray[copyArray.length - 1] = element;
        array = copyArray;
        return copyArray;
    }
}

class Array {
    private Float[] array;

    Array(Float[] array) {
        this.array = array;
    }

    Float[] addArrays(Float[] array1, Float[] oneElement, Float[] array2) {
        Float[] result = new Float[array1.length + 1 + array2.length];
        System.arraycopy(array1, 0, result, 0, array1.length);
        System.arraycopy(oneElement, 0, result, array1.length, 1);
        System.arraycopy(array2, 0, result, array1.length + 1, array2.length);
        return result;
    }

    Float[] quickSort() {
        if (array.length < 2) {
            return array;
        } else {
            Float first = array[0];
            upgradeArray min = new upgradeArray();
            upgradeArray max = new upgradeArray();
            Float[] kogoto1 = min.getArray();
            Float[] kogoto2 = max.getArray();
            for (int i = 1; i < array.length; i++) {
                if (array[i] < first) {
                    kogoto1 = min.add(array[i]);

                }
            }
            for (int i = 0; i < array.length; i++) {
                if (array[i] > first) {
                    kogoto2 = max.add(array[i]);
                }
            }
            Float[] oneElement = {first};
            return addArrays(new Array(kogoto1).quickSort(), oneElement, new Array(kogoto2).quickSort());
        }
    }
}

public class BinarySearch {
    public static void main(String[] args) {
        Float[] array1 = {(float) 5, (float) 2, (float) 3, (float) 3, (float) -1, (float) 0, (float) 1212};
        System.out.println(Arrays.toString(array1));
        Array array = new Array(array1);
        System.out.println(Arrays.toString(array.quickSort()));


    }
}