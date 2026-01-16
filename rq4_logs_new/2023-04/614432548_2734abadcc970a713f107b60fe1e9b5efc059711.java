package org.example.homework_nr_4;

import static java.lang.System.arraycopy;

public class HomeWork4 {

    public static void main(String[] args) {
// Задание 4
// Инициализация одномерного массива
        int[] array = new int[]{4, 0, -1, 5, 12, 23, 11};
        for (int index = 0; index != array.length; index++) {
//Вывод элементов массива на коноль
            System.out.print(array[index]+"  ");
        }

        System.out.println();

//Задание 5
//        Инициализация одномерного массива типа int
//                и двумерного массива типа int

        int[] anotherarray = new int[]{9,8,7,6,5,4};

            int [][] bidiamationarray = new int[][] {
                    {3,7,14,8,-2},
                    {9,35,1,88,1},
                    {2,6,8,10,12}
            };
// Вычесление суммы и среднего значения элементов одномерного массива
        float sum = 0;
        float count=0;
        float avarage =0;
        for (int j = 0; j != anotherarray.length; j++) {
              sum += anotherarray[j];
        }
        for (int j = 0; j != anotherarray.length; j++) {
            count++;
        }
        avarage = sum/count;

        System.out.println("сумма элементов одномерного массива равна  "+ sum);
        System.out.println("среднее значение элементов одномерного массива равно   "+ avarage);
//цикл для вывода элементов двухмерного массива на консоль
            for (int j= 0; j != bidiamationarray.length; j++) {
                for (int i= 0; i != bidiamationarray[j].length; i++) {
                System.out.print(bidiamationarray[j][i] +" ");
            }
                System.out.println();
            }
//вычисление количества четных и нечетных элементов двухмерного массива
        int chetnelement = 0;
        int Nechetnelement = 0;
        for (int j = 0; j != bidiamationarray.length; j++) {
            for (int i= 0; i != bidiamationarray[j].length; i++) {
                if(bidiamationarray[j][i] % 2==0) { chetnelement ++;}
                    else {if (!(bidiamationarray[j][i] % 2==0)) {
                        Nechetnelement++;}
                    }
                }
            }

        System.out.println("количества четных элементов двухмерного массива ="+ chetnelement);
        System.out.println("количества нечетных элементов двухмерного массива ="+ Nechetnelement);

        // Бонус
//        Инициализация массива типа String и заполнение его именами коллег
        String[] namearray = new String[]{"Nikita", "Alexei", "Ivan", "Timofei"};
        for (int index = 0; index != namearray.length; index++) {

            System.out.println(namearray[index]+" ");}

        String [] Otherarray = new String[4];

        System.out.println("=============================================================");
//коирование всех элеменов массива в новый массив

        arraycopy(namearray,0,Otherarray,0,4);
        for (int k=0; k != Otherarray.length; k++) {
            System.out.println(Otherarray[k] + " ");
        }

        }
    }