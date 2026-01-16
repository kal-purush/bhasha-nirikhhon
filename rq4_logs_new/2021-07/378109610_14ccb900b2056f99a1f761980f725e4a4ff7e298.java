import java.util.Random;
import java.util.Scanner;

public class Task4 {
    public Task4() {
    }

    public static void main(String[] args) {
        System.out.println(summ(-6, 4));
        System.out.println(summ1(-6, -5));
        System.out.println(getCountsOfDigits(23459L));
        progressionA(1);
        inLine();
        maxIndex();
        massZero();
        int[] var1 = findMaxElementAndReplace(new int[]{4, 5, 0, 23, 77, 0, 8, 9, 101, 2});
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            int i = var1[var3];
            System.out.print(i + " ");
        }

        System.out.println(" ");
//        massDifference();
        matrixTransp();
        paintMatrix();
    }

    private static void progressionA(int ameba) {
        System.out.println("Решение задачи 1");

        for (int i = 0; i < 24; i += 3) {
            ameba *= 2;
            System.out.println(ameba);
        }

    }

    public static int summ(int a, int b) {
        int result = a;
        if (a != 0 && b != 0) {
            for (int i = 1; i < Math.abs(b); ++i) {
                result += a;
            }

            if (a > 0 ^ b > 0) {
                result = -Math.abs(result);
            } else {
                result = Math.abs(result);
            }
        } else {
            result = 0;
        }

        return result;
    }

    public static int summ1(int a, int b) {
        if (a != 0 && b != 0) {
            if (a == 1) {
                return b;
            } else if (b == 1) {
                return a;
            } else if (a < 0 && b < 0) {
                return summ1(a * -1, b * -1);
            } else if (a < 0) {
                return -1 * summ1(a * -1, b);
            } else {
                return b < 0 ? -1 * summ1(a, b * -1) : a + summ1(a, b - 1);
            }
        } else {
            return 0;
        }
    }

    public static void paintMatrix() {
        System.out.print("Введите размер матрицы: ");
        Scanner key = new Scanner(System.in);
        int n = key.nextInt();

        int i;
        int j;
        for (i = 0; i <= n; ++i) {
            for (j = 0; j < n - i; ++j) {
                System.out.print("  ");
            }

            for (j = i; j > 0; --j) {
                System.out.print("* ");
            }

            System.out.println();
        }

        for (i = 0; i <= n; ++i) {
            for (j = i; j > 0; --j) {
                System.out.print("* ");
            }

            System.out.println();
        }

        System.out.println();

        for (i = n; i >= 1; --i) {
            if (i != n) {
                for (j = 1; j <= 0; ++j) {
                    System.out.print(" ");
                }
            }

            for (j = i; j >= 1; --j) {
                System.out.print("*");
                if (i != 1) {
                    System.out.print(" ");
                }
            }

            System.out.println();
        }

        for (i = 0; i < n; ++i) {
            for (j = 0; j < i; ++j) {
                System.out.print(" ");
            }

            for (j = i; j < n; ++j) {
                System.out.print("*");
            }

            System.out.println();
        }

    }

    public static int getCountsOfDigits(long number) {
        System.out.println("Решение задачи 4");
        if (number == 0L) {
            System.out.print(number + " - ноль");
        } else if (number > 0L) {
            System.out.print(number + " - это положительное число, количество цифр = " + String.valueOf(Math.abs(number)).length());
        } else {
            System.out.print(number + " - это отрицательное число, количество цифр = " + String.valueOf(Math.abs(number)).length());
        }

        return (int) number;
    }

    public static void inLine() {
        System.out.println("Решение задачи 5");
        int[] mas = new int[50];
        int i = 0;

        for (int n = 1; i < mas.length; ++i) {
            mas[i] = n;
            System.out.print(mas[i] + " ");
            n += 2;
        }

        System.out.println();

        for (i = mas.length - 1; i >= 0; --i) {
            System.out.print(mas[i] + " ");
        }

    }

    public static void maxIndex() {
        System.out.println("");
        System.out.println("Решение задачи 6");
        int[] mass = new int[12];
        int max = 0;
        int j = 0;

        int i;
        for (i = 0; i < mass.length; ++i) {
            mass[i] = (int) (0.0D + Math.random() * 15.0D);
            System.out.print(mass[i] + " ");
        }

        for (i = 0; i < mass.length; ++i) {
            if (mass[i] >= max) {
                max = mass[i];
                j = i;
            }
        }

        System.out.println("");
        System.out.println("Максимальный элемент " + max + " ,индекс его последнего вхождения в массив= " + j);
    }

    public static void massZero() {
        System.out.println("Решение задачи 7");
        int[] mass = new int[20];

        int i;
        for (i = 0; i < mass.length; ++i) {
            mass[i] = (int) (0.0D + Math.random() * 20.0D);
        }

        for (i = 0; i < mass.length; ++i) {
            System.out.print(mass[i] + " ");
        }

        System.out.println();

        for (i = 0; i < mass.length; ++i) {
            if (i % 2 == 0) {
                mass[i] = 0;
            }

            System.out.print(mass[i] + " ");
        }

        System.out.println(" ");
    }

    public static int[] findMaxElementAndReplace(int[] array) {
        System.out.println("Решение задачи 8");
        int maxIndex = 0;

        for (int i = 1; i < array.length; ++i) {
            if (array[i] > array[maxIndex]) {
                maxIndex = i;
            }
        }

        int replaseIndex = 0;
        if (maxIndex != replaseIndex) {
            int t = array[replaseIndex];
            array[replaseIndex] = array[maxIndex];
            array[maxIndex] = t;
        }

        return array;
    }

//    public static void massDifference() {
//        System.out.println("Решение задачи 9");
//        int[] arr1 = new int[]{123, 2, 12, 4, 3, 6, 8, 5, 8};
//        int[] arr2 = new int[]{356, 5, 7, 1, 3, 8, 2, 2, 9, 10, 12};
//        int[] arr3 = ((Map)Stream.of(arr1, arr2).flatMap((arr) -> {
//            return Arrays.stream(arr).distinct().boxed();
//        }).collect(Collectors.groupingBy((i) -> {
//            return i;
//        }, Collectors.counting()))).entrySet().stream().filter((e) -> {
//            return (Long)e.getValue() > 1L;
//        }).map((e) -> {
//            return (Integer)e.getKey();
//        }).mapToInt(Integer::intValue).toArray();
//        System.out.println(Arrays.toString(arr3));
//    }

    public static void matrixTransp() {
        System.out.println("Решение задачи 10");
        Scanner keyboard = new Scanner(System.in);
        System.out.print("Введите  размер стороны квадрата матрицы: ");
        int size = keyboard.nextInt();
        int[][] matrix = new int[size][size];
        new Random();

        int i;
        int j;
        for (i = 0; i < matrix.length; ++i) {
            for (j = 0; j < matrix[i].length; ++j) {
                matrix[i][j] = (int) (0.0D + Math.random() * 50.0D);
            }
        }

        for (i = 0; i < matrix.length; ++i) {
            for (j = 0; j < matrix[i].length; ++j) {
                System.out.print(matrix[i][j] + " ");
            }

            System.out.println();
        }

        for (i = 0; i < matrix.length; ++i) {
            for (j = i + 1; j < matrix.length; ++j) {
                int temp = matrix[i][j];
                matrix[i][j] = matrix[j][i];
                matrix[j][i] = temp;
            }
        }

        System.out.println();
        System.out.println("Транспонированная матрица:");

        for (i = 0; i < matrix.length; ++i) {
            for (j = 0; j < matrix.length; ++j) {
                System.out.printf("%5d", matrix[i][j]);
            }

            System.out.println();
        }

    }
}