package ru.geekbrains.java3home6;

import java.util.ArrayList;
import java.util.List;

public class Arr {
        public static int[] findNumbersAfterLastFourAtArray(int[] startArray) {
            List<Integer> resultList = new ArrayList<>();
            int temp = 0;
            boolean check = false;
            for (int i = 0; i < startArray.length; i++) {
                if (startArray[i] == 4) {
                    temp = i + 1;
                    check = true;
                }
            } if (!check) {
                throw new RuntimeException("SWW");
            }

            for (int i = temp; i < startArray.length; i++) {
                resultList.add(startArray[i]);
            }

            int[] resultArray = new int[resultList.size()];
            for (int i = 0; i < resultList.size(); i++) {
                resultArray[i] = resultList.get(i);
            }

            for (int i : resultArray) {
                System.out.print(i + " ");
            }
            System.out.println();
            System.out.println(resultList.toString());

            return resultArray;

        }

        public static boolean checkArrayForOneAndFour(int[] startArray) {
            for (int i = 0; i < startArray.length; i++) {
                if ((startArray[i] == 1)  || (startArray[i] == 4)) {
                    return true;
                }
            }
            return false;
        }

    }