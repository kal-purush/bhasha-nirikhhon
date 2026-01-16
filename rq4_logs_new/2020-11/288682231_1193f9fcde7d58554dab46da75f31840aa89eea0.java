package ru.moore;

import java.util.ArrayList;
import java.util.Arrays;

public class Task2 {

    public static void main(String[] args) {
        Integer[] arrayInt = {1, 2};
        String[] arrayStr = {"1", "2"};

        ToArrayList<Integer> changeInt = new ToArrayList<Integer>(arrayInt);
        changeInt.change();
        System.out.println(Arrays.toString(arrayInt));

        ToArrayList<String> changeStr = new ToArrayList<String>(arrayStr);
        changeStr.change();
        System.out.println(Arrays.toString(arrayStr));
    }

}

class ToArrayList<T>{

    private T[] toArrayList;

    public ToArrayList(T[] toArrayList) {
        this.toArrayList = toArrayList;
    }

    public ArrayList change (){
        ArrayList<T> temp = new ArrayList<>();
        for (int i = 0; i < toArrayList.length; i++) {
            temp.add(toArrayList[i]);
        }
        return temp;
    }
}